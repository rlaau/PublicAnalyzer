### 결론
1. 알고리즘이 내놓는 답은 반드시 맞음
2. 그러나 답을 못 내놓을 경우도 이음.
3. 이는 "CREATE2:같은 EOA/컨트랙트가 salt만 바꿔서 여러 주소를 미리 “예약” 가능." 이라는 2019년 이후의 하드포크 이후의 경우에 해당함.

디앱(예: Uniswap V3)에서 이걸 활용해서 배포 전에 “컨트랙트 주소를 먼저 알려주고” 코드를 심습니다.

### "검증된" 컨트렉트 추론 알고리즘
1. 일단 GPT의 도움 받음
2. 그러나 여전히 검증이 필요는 했었고, 실제 샘플 1만개로 검증함
### 결과 해석 요약
1. 모든 기간에 대해 알고리즘이 일반화됨.
### 검증 결과 Raw
2025/08/24 15:44:08 Selected 100 files (target 100 each => up to 10000 checks)
✅ OK  [txc_2024-02-14.mm idx=544]
  from=0xb58d42a74fc2d60beec6fa5d7ca784e723cc1024
  nonce=5
  expected(rcaddr)=0xaba615044d5640bd151a1b0bdac1c04806af1ad5
  predicted=0xaba615044d5640bd151a1b0bdac1c04806af1ad5

✅ OK  [txc_2024-02-14.mm idx=18]
  from=0x28b132ff9fd3b880ba3597d219ff1beb733b0c0f
  nonce=0
  expected(rcaddr)=0x3363da92b4ac96c56675cbec3353c8a29e143c25
  predicted=0x3363da92b4ac96c56675cbec3353c8a29e143c25

✅ OK  [txc_2024-02-14.mm idx=395]
  from=0xd69fefe5df62373dcbde3e1f9625cf334a2dae78
  nonce=1564
  expected(rcaddr)=0x7341137f512098c62e11c5509f51cca4fc5cac67
  predicted=0x7341137f512098c62e11c5509f51cca4fc5cac67

✅ OK  [txc_2024-02-14.mm idx=371]
  from=0x422feb0c22f25d88e61db99fa6b5a1f96deb97b1
  nonce=31
  expected(rcaddr)=0xf6af11913a27c825ae272a0e9deffae8571857c6
  predicted=0xf6af11913a27c825ae272a0e9deffae8571857c6

✅ OK  [txc_2024-02-14.mm idx=364]
  from=0xb4533c8adec4b2773212ca62d4101fa385f6c630
  nonce=0
  expected(rcaddr)=0xcd152e2a8c19bbfc2c89c5d9b19e53ab17a29b2a
  predicted=0xcd152e2a8c19bbfc2c89c5d9b19e53ab17a29b2a

✅ OK  [txc_2024-02-14.mm idx=135]
  from=0xa6364f394616dd9238b284cff97cd7146c57808d
  nonce=231
  expected(rcaddr)=0x2bb64d96b0dcfab7826d11707aae3f55409d8e19
  predicted=0x2bb64d96b0dcfab7826d11707aae3f55409d8e19

✅ OK  [txc_2024-02-14.mm idx=547]
  from=0x7ab003a9c7cd3016e90a03a4a608975a6fac40b4
  nonce=36
  expected(rcaddr)=0x9f05f97244b66ca8092517dce3fecc5157d4af49
  predicted=0x9f05f97244b66ca8092517dce3fecc5157d4af49

✅ OK  [txc_2024-02-14.mm idx=312]
  from=0xd69fefe5df62373dcbde3e1f9625cf334a2dae78
  nonce=1566
  expected(rcaddr)=0xdd6921d8f66ddd1a24e230ebb3de8467c18181f5
  predicted=0xdd6921d8f66ddd1a24e230ebb3de8467c18181f5

✅ OK  [txc_2024-02-14.mm idx=473]
  from=0x04ed7c74b911526f5ef4d8e3ad5dec77aa2747e9
  nonce=0
  expected(rcaddr)=0xceb677dbbd22d75c2a300eda2b27f2d83f744a12
  predicted=0xceb677dbbd22d75c2a300eda2b27f2d83f744a12

✅ OK  [txc_2024-02-14.mm idx=125]
  from=0x422feb0c22f25d88e61db99fa6b5a1f96deb97b1
  nonce=33
  expected(rcaddr)=0x0b582a6798b325d2c44513c170d24a6d04a65c6f
  predicted=0x0b582a6798b325d2c44513c170d24a6d04a65c6f

2025/08/24 15:44:08 [DONE] txc_2024-02-14.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2020-10-10.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2024-10-19.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2024-06-19.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2019-04-05.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2022-12-07.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2022-08-05.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2023-07-24.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2020-10-13.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2017-02-26.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2018-04-27.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2015-10-27.mm -> sampled=97 correct=97 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2017-04-30.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2019-07-26.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2022-01-15.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2021-02-21.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2019-07-09.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2017-11-01.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2021-10-30.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2018-09-27.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2021-08-28.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2024-04-14.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2016-02-18.mm -> sampled=74 correct=74 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2019-05-07.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2018-01-10.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2016-07-26.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2017-12-20.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2019-11-20.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2020-04-08.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2017-12-23.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2021-09-26.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2020-01-07.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2021-07-15.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2019-04-18.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2018-02-15.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2019-12-08.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2017-12-18.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2023-08-16.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2023-12-26.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2023-02-23.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2024-08-08.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2019-10-20.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2020-08-04.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2019-10-12.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2017-06-03.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2018-11-29.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2017-07-13.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2020-10-16.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2016-10-07.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2018-05-21.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2023-02-18.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2024-08-17.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2022-05-31.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2022-07-08.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2023-11-01.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2019-06-28.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2017-02-25.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2016-11-03.mm -> sampled=71 correct=71 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2021-05-28.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2022-06-02.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2023-03-16.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2018-04-21.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2018-05-24.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2020-11-02.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2019-06-23.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2023-03-17.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2024-06-30.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2020-08-24.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2019-10-28.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2018-09-20.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2020-03-02.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2016-04-08.mm -> sampled=99 correct=99 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2020-06-14.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2016-09-12.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2022-01-05.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2017-01-24.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2020-07-09.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2024-02-09.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2019-07-15.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2022-01-16.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2020-12-18.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2024-07-17.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2019-04-06.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2015-10-13.mm -> sampled=53 correct=53 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2020-08-05.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2021-12-01.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2020-05-06.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2020-09-13.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2021-08-11.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2022-04-05.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2023-07-11.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2020-12-26.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2019-06-17.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2024-08-16.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2021-06-10.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2023-08-03.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2022-01-03.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2023-11-17.mm -> sampled=100 correct=100 incorrect=0
2025/08/24 15:44:08 [DONE] txc_2016-03-17.mm -> sampled=86 correct=86 incorrect=0

Summary:
  Total checked: 9780
  Correct: 9780 (100.00%)
  Incorrect: 0 (0.00%)
