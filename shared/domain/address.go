package domain

import "encoding/hex"

// Address는 이더리움 주소를 나타내는 타입입니다.
type Address [20]byte

// ✅ 문자열 변환 (0x + hex encoding)
func (a Address) String() string {
	return "0x" + hex.EncodeToString(a[:])
}

type PredefinedAddress interface {
	MinerAddress |
		CexHotWalletAddress | CexColdWalletAddress |
		ERC20TokenAddress |
		LendingPoolAddress | LendingStakingTokenAddress |
		SwapLiquidityPoolAddress | LiquidityPoolStakingTokenAddress |
		RouterAddress |
		BeaconDepositAddress |
		NFTContractAddress |
		BridgeAddress
}

type DefinedOnProcess interface {
	UserAddress | CexDepositAddress
}

type UserAddress Address // 사용자 계정 (외부 소유 지갑)

type MinerAddress Address // 채굴자 주소 (마이닝 보상 수령 주소)

// Centralized Exchange (CEX)
type CexDepositAddress Address    // 거래소 입금주소 (사용자별 생성)
type CexHotWalletAddress Address  // 거래소 운영용 지갑 (빠른 출금용)
type CexColdWalletAddress Address // 거래소 장기 보관 지갑

type ERC20TokenAddress Address // ERC-20 토큰 컨트랙트 주소

// DeFi Lending/Staking 컨트랙트
type LendingPoolAddress Address         // Aave 등 Lending Pool 주소
type LendingStakingTokenAddress Address // Lending 예치 후 지급되는 이자토큰 (예: aETH, cETH 등)

// DeFi Swap 관련 컨트랙트
type SwapLiquidityPoolAddress Address         // Uniswap, SushiSwap 등 AMM 컨트랙트 주소
type LiquidityPoolStakingTokenAddress Address // LP토큰 컨트랙트 주소 (Uniswap LP토큰 등)

type RouterAddress Address // Uniswap, SushiSwap 등 라우터 컨트랙트 주소
// ETH 2.0 (Beacon Chain)
type BeaconDepositAddress Address // ETH2.0 스테이킹 주소

// NFT 관련
type NFTContractAddress Address // ERC-721, ERC-1155 컨트랙트 주소

// Bridge 관련
type BridgeAddress Address // Arbitrum, Optimism 등 브릿지 컨트랙트 주소

type UndefinedAddress Address // 정의되지 않은 주소
