package dto

// 이벤트/Tx 타입 (예시)

type CcEvt struct{ Cmd, Arg string }
type EeEvt struct{ Note string }
type CceEvt struct { /* ... */
}
type EecEvt struct { /* ... */
}
