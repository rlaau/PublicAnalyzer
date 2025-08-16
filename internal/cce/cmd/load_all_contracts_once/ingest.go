package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/rlaaudgjs5638/chainAnalyzer/shared/groundknowledge/chaintimer"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type Config struct {
	ProjectID      string
	UsePrecomp     bool   // true면 precompDataset 사용
	PrecompDataset string // 예: "your-project.precomp"

	// 출력
	OutputFile string // JSONL 파일 경로(예: ./contracts.jsonl)

	// 체크포인트 파일
	CheckpointPath string

	// 슬라이스/비용 가드
	SliceStep          time.Duration // 권장 15m~60m
	BatchLimit         int           // 5k~10k
	Delay              time.Duration // 최신영역 지연 버퍼 (2~5m)
	MaximumBytesBilled int64         // per job 상한 (GiB 단위 변환해서 넣기)
	DryRun             bool          // 드라이런이면 쓰기/체크포인트 저장 안함
	MaxSlices          int           // 드라이런 안전 제한(0=무제한)
	MaxRows            int           // 드라이런 안전 제한(0=무제한)

	// 인증 모드
	// "user": 내 구글 계정(브라우저 로그인, 토큰 캐시)
	// "adc" : ADC(기본값 폴백)
	AuthMode string
}

type ckpt struct {
	LastTS   time.Time `json:"last_ts"`
	LastAddr string    `json:"last_addr"`
	HasCkpt  bool      `json:"has_ckpt"`
}

// ---- 체크포인트 helpers
func loadCkpt(path string) (ckpt, error) {
	if path == "" {
		return ckpt{}, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return ckpt{}, nil
	}
	var c ckpt
	_ = json.Unmarshal(b, &c)
	return c, nil
}
func saveCkpt(path string, c ckpt) error {
	if path == "" {
		return nil
	}
	b, _ := json.MarshalIndent(c, "", "  ")
	return os.WriteFile(path, b, 0644)
}

// ---- BigQuery 클라이언트 (브라우저 로그인 기반 OAuth2)
func newBQClientUserOAuth(ctx context.Context, projectID string) (*bigquery.Client, error) {
	clientID := os.Getenv("GOOGLE_OAUTH_CLIENT_ID")
	clientSecret := os.Getenv("GOOGLE_OAUTH_CLIENT_SECRET")
	if clientID == "" || clientSecret == "" {
		return nil, fmt.Errorf("set GOOGLE_OAUTH_CLIENT_ID and GOOGLE_OAUTH_CLIENT_SECRET")
	}

	redirectURL := os.Getenv("OAUTH_REDIRECT_URL")
	if redirectURL == "" {
		// loopback 방식(권장)
		redirectURL = "http://127.0.0.1:8765/callback"
	}

	cfg := &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		RedirectURL:  redirectURL,
		Scopes: []string{
			"https://www.googleapis.com/auth/bigquery",
			"https://www.googleapis.com/auth/cloud-platform",
		},
		Endpoint: google.Endpoint,
	}

	// 로컬 토큰 캐시
	tokenPath := ".user_oauth_token.json"
	var tok *oauth2.Token
	if b, err := os.ReadFile(tokenPath); err == nil {
		_ = json.Unmarshal(b, &tok)
	}

	if tok == nil {
		// 최초 로그인 플로우
		state := randState()
		authURL := cfg.AuthCodeURL(state, oauth2.AccessTypeOffline, oauth2.ApprovalForce)

		log.Printf("[auth] Opening browser for Google login…")
		_ = openBrowser(authURL)
		log.Printf("[auth] If browser didn't open, paste this URL in your browser:\n%s", authURL)

		// 콜백 서버 띄우기
		codeCh := make(chan string, 1)
		mux := http.NewServeMux()
		mux.HandleFunc("/callback", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("state") != state {
				http.Error(w, "state mismatch", http.StatusBadRequest)
				return
			}
			code := r.URL.Query().Get("code")
			fmt.Fprintln(w, "Login complete. You can close this window.")
			codeCh <- code
		})

		ln, err := net.Listen("tcp", strings.TrimPrefix(redirectURL, "http://"))
		if err != nil {
			return nil, fmt.Errorf("listen on %s: %w", redirectURL, err)
		}
		defer ln.Close()
		srv := &http.Server{Handler: mux}
		go func() {
			_ = srv.Serve(ln)
		}()
		code := <-codeCh
		_ = srv.Shutdown(ctx)

		var errEx error
		tok, errEx = cfg.Exchange(ctx, code)
		if errEx != nil {
			return nil, fmt.Errorf("token exchange failed: %w", errEx)
		}
		// 캐시 저장
		if b, err := json.Marshal(tok); err == nil {
			_ = os.WriteFile(tokenPath, b, 0600)
		}
	}

	// 자동 리프레시 TokenSource
	ts := cfg.TokenSource(ctx, tok)
	return bigquery.NewClient(ctx, projectID, option.WithTokenSource(ts))
}

func randState() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return base64.RawURLEncoding.EncodeToString(b[:])
}

func openBrowser(url string) error {
	switch runtime.GOOS {
	case "windows":
		return exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		return exec.Command("open", url).Start()
	default: // linux
		return exec.Command("xdg-open", url).Start()
	}
}

// ---- 실행 1회 (드라이런 추정 + 시간창 자동 축소 + 초과 시 실행 차단)
func RunOnce(ctx context.Context, cfg Config) error {
	pid := cfg.ProjectID
	if pid == "" {
		pid = os.Getenv("GOOGLE_CLOUD_PROJECT")
	}
	if pid == "" {
		return fmt.Errorf("missing project id")
	}

	if cfg.SliceStep <= 0 {
		cfg.SliceStep = 30 * time.Minute
	}
	if cfg.BatchLimit <= 0 {
		cfg.BatchLimit = 5000
	}
	if cfg.Delay <= 0 {
		cfg.Delay = 2 * time.Minute
	}
	if cfg.MaximumBytesBilled <= 0 {
		cfg.MaximumBytesBilled = 20 << 30 // 20GiB
	}
	if cfg.AuthMode == "" {
		cfg.AuthMode = "user" // 기본을 사용자 OAuth로
	}

	// BigQuery 클라이언트
	var bq *bigquery.Client
	var err error
	switch cfg.AuthMode {
	case "user":
		bq, err = newBQClientUserOAuth(ctx, pid)
	case "adc":
		bq, err = bigquery.NewClient(ctx, pid) // ADC 폴백
	default:
		return fmt.Errorf("unknown AuthMode: %s", cfg.AuthMode)
	}
	if err != nil {
		return err
	}
	defer bq.Close()

	// 범위: ckpt.LastTS ~ (now-Delay)
	ck, _ := loadCkpt(cfg.CheckpointPath)
	from := ck.LastTS
	if from.IsZero() {
		// 최초 실행: 너무 멀리 가지 않게 최근 24h부터 시작(필요시 ckpt로 오버라이드)
		from = time.Now().UTC().Add(-24 * time.Hour)
	}
	to := time.Now().UTC().Add(-cfg.Delay)
	if !to.After(from) {
		log.Printf("[ingest] up-to-date: from=%s to=%s", from, to)
		return nil
	}

	sql := BuildContractBatchSQL(cfg.UsePrecomp, cfg.PrecompDataset)

	totalRows := 0
	slices := 0
	lastTS := ck.LastTS
	lastAddr := ck.LastAddr
	hasCkpt := ck.HasCkpt

	const minSliceStep = 5 * time.Minute // 시간창 줄일 때 이 이하로는 더 줄이지 않음

	// JSONL writer
	var jsonFile *os.File
	var enc *json.Encoder
	if !cfg.DryRun && cfg.OutputFile != "" {
		jsonFile, err = os.OpenFile(cfg.OutputFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		defer jsonFile.Close()
		enc = json.NewEncoder(jsonFile)
	}

	for cur := from; cur.Before(to); cur = cur.Add(cfg.SliceStep) {
		sliceEnd := cur.Add(cfg.SliceStep)
		if sliceEnd.After(to) {
			sliceEnd = to
		}
		slices++

		// 이 슬라이스는 가변적으로 줄일 수 있으니 별도 변수 사용
		curStart := cur
		curEnd := sliceEnd

	retrySlice:
		for {
			q := bq.Query(sql)
			q.Location = "US"
			q.Parameters = []bigquery.QueryParameter{
				{Name: "start_ts", Value: curStart},
				{Name: "end_ts", Value: curEnd},
				{Name: "last_ts", Value: lastTS},
				{Name: "last_addr", Value: lastAddr},
				{Name: "has_ckpt", Value: hasCkpt},
				{Name: "lim", Value: int64(cfg.BatchLimit)},
			}

			// 1) 드라이런으로 예상 바이트 파악
			q.DryRun = true
			dryJob, err := q.Run(ctx)
			if err != nil {
				return err
			}
			st, err := dryJob.Status(ctx)
			if err != nil {
				return err
			}
			var est int64
			if st != nil && st.Statistics != nil {
				est = st.Statistics.TotalBytesProcessed
			}
			q.DryRun = false

			// 1-1) 예상 바이트가 상한 초과면 시간창 절반으로 줄이며 재시도
			if est > cfg.MaximumBytesBilled {
				win := curEnd.Sub(curStart)
				if win > minSliceStep {
					newDur := win / 2
					if newDur < minSliceStep {
						newDur = minSliceStep
					}
					curEnd = curStart.Add(newDur)
					log.Printf("[guard] est=%d > cap=%d; shrink window to %s ~ %s",
						est, cfg.MaximumBytesBilled,
						curStart.Format(time.RFC3339), curEnd.Format(time.RFC3339))
					goto retrySlice
				}
				// 더 못 줄이면 안전을 위해 실행 중단(비용 가드)
				return fmt.Errorf("[guard] estimated bytes %d exceed cap %d even at min window (%s~%s). "+
					"Increase cap or reduce SliceStep/BatchLimit",
					est, cfg.MaximumBytesBilled,
					curStart.Format(time.RFC3339), curEnd.Format(time.RFC3339))
			}

			// 2) 실제 실행
			job, err := q.Run(ctx)
			if err != nil {
				return err
			}
			it, err := job.Read(ctx)
			if err != nil {
				return err
			}

			// 3) 수집
			rows := 0
			for {
				var r struct {
					Address        string    `bigquery:"address"`
					Creator        string    `bigquery:"creator"`
					CreationTx     string    `bigquery:"creation_tx"`
					CreatedAt      time.Time `bigquery:"created_at"`
					CreatedBlock   int64     `bigquery:"created_block"`
					BytecodeSHA256 string    `bigquery:"bytecode_sha256"`
					IsERC20        bool      `bigquery:"is_erc20"`
					IsERC721       bool      `bigquery:"is_erc721"`
					// FunctionSighashes []string  `bigquery:"function_sighashes"`
					Symbol   *string  `bigquery:"symbol"`
					Name     *string  `bigquery:"name"`
					Decimals *int64   `bigquery:"decimals"`
					Owner    *string  `bigquery:"owner"`
					OwnerLog []string `bigquery:"ownerlog"`
				}
				if err := it.Next(&r); err == iterator.Done {
					break
				}
				if err != nil {
					return err
				}

				// JSONL로 바로 쓴다(정규화 포함)
				obj := map[string]any{
					"address":      strings.ToLower(r.Address),
					"creator":      strings.ToLower(r.Creator),
					"creation_tx":  strings.ToLower(r.CreationTx),
					"created_at":   chaintimer.ChainTime(r.CreatedAt),
					"is_erc20":     r.IsERC20,
					"is_erc721":    r.IsERC721,
					"last_seen_at": chaintimer.ChainTime(time.Now().UTC()),
				}
				if r.Symbol != nil {
					obj["symbol"] = *r.Symbol
				}
				if r.Name != nil {
					obj["name"] = *r.Name
				}
				if r.Decimals != nil {
					obj["decimals"] = r.Decimals
				}
				if r.Owner != nil && *r.Owner != "" {
					o := strings.ToLower(*r.Owner)
					if strings.HasPrefix(o, "0x") {
						obj["owner"] = o
					}
				}
				if len(r.OwnerLog) > 0 {
					obj["ownerlog"] = r.OwnerLog
				}

				// 드라이런이 아니면 파일에 기록
				if !cfg.DryRun && enc != nil {
					if err := enc.Encode(obj); err != nil {
						return err
					}
				}

				rows++
				lastTS, lastAddr, hasCkpt = r.CreatedAt, r.Address, true

				// 드라이런 안전 제한
				if cfg.DryRun && cfg.MaxRows > 0 && (totalRows+rows) >= cfg.MaxRows {
					break
				}
			}

			// 통계 로그
			if st2, _ := job.Status(ctx); st2 != nil && st2.Statistics != nil {
				log.Printf("[ingest] %s~%s rows=%d bytes=%d (dry=%v)",
					curStart.Format(time.RFC3339), curEnd.Format(time.RFC3339),
					rows, st2.Statistics.TotalBytesProcessed, cfg.DryRun)
			}

			totalRows += rows

			// 페이지(=주소 기준 페이지네이션) 끝 조건
			if rows < cfg.BatchLimit {
				break
			}
			if cfg.DryRun && cfg.MaxRows > 0 && totalRows >= cfg.MaxRows {
				break
			}
			// rows == BatchLimit 이면 동일 시간창에서 다음 페이지 계속(ckpt로 진행)
		}

		if cfg.DryRun && cfg.MaxSlices > 0 && slices >= cfg.MaxSlices {
			break
		}
	}

	// 체크포인트 저장
	if !cfg.DryRun && hasCkpt {
		if err := saveCkpt(cfg.CheckpointPath, ckpt{
			LastTS: lastTS, LastAddr: lastAddr, HasCkpt: hasCkpt,
		}); err != nil {
			return err
		}
	}
	log.Printf("[ingest] done: slices=%d totalRows=%d dry=%v", slices, totalRows, cfg.DryRun)
	return nil
}
