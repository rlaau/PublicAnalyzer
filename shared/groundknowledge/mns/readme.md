### Module Name System
모니터나 서버가 해당 모듈, 구조체 정보 알려 할 때 쓰기 위한 "이름 제공"시스템임.
### How To Use
"모듈"+"구조체 이름"기반으로 "구조체 ID"를 등록 후 받을 수 있음.
이걸 바탕으로, 해당 구조체가 요구한 "내 자원 좀 줘"를 가져오기 가능.
### Example
// 방식 1: 구조체가 있을 때
dbRequest1 := NewResourceRequest(userService, "database", "users_db")

// 방식 2: 구조체 없이 문자열만으로
dbRequest2 := NewResourceRequestByNames("UserService", "UserService", "database", "users_db") 

// 둘 다 같은 ModuleID를 가짐!