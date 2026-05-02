## Introduction Video Link
1. YouTube presentation video: https://www.youtube.com/watch?v=9_xKRzRfsUY&t=1033s

## Problem and Goal
1. Problem: The “anonymity” of blockchain can be abused as a tool for fraud. In particular, scammers repeatedly create new wallets and use them to carry out token scams.
2. Goal: By clustering a user’s wallets, if the user creates a new wallet for fraud, we can identify and flag that wallet early before the scam happens.
3.  

## Features
1. Processes and applies triplet logic to cluster a wallet user’s alternate accounts.
2. Based on this information, tracks all wallets created by “one user.”

## Demo Images
1. Investigated alternate accounts of a specific well-known account.
2. This image shows the contracts created by that account and the supporting transactions behind the finding.

<img width="1993" height="1403" alt="image" src="https://github.com/user-attachments/assets/a22394db-d8b3-4b02-a0e4-b678711d9193" />


### Internal Module Structure

Model Design
1. domain/d_model.go:
Models the domain logic that the module should contain.
2. app/a_model.go:
Models the business logic that the module should contain.
3. iface/i_model.go:
Models the infrastructure that the module depends on.

Folder Design
1. domain:
Defines the entities, values, events, and aggregates managed by the module.
The core domain logic is modeled in d_model.go.
2. iface:
Used to isolate dependencies on infra and app.
3. infra:
Provides the infrastructure used by app.
Implemented based on the models defined in iface.
4. app:
Responsible for business logic.
Implemented based on external ports and a_model.go.

## Module Design

### internal

1. DexFeeder:
(dexSource) → {dexFeeder} → (dexAN)

Responsible for fetching raw DEX indicators.

2. DxAn:
(dexFeeder) → {dxAn} → (interpreter)

Converts raw DEX indicators into meaningful DEX metrics.
During analysis, it uses multiple submodules to extract higher-dimensional metrics.

   1. Receives “which DEX indicators to analyze” as settings and implements them as submodules.
   A task loop that implements arbitrary settings may also be added as a supporting component.

   2. Based on those settings, it receives real-time DEX data, processes it, and passes it to the interpreter.

3. CexFeeder (not implemented):
(cexSource) → {cexFeeder} → (cxAn)

Responsible for fetching raw CEX indicators.

4. CxAn (not implemented):
(cexFeeder) → {cxAn} → (interpreter)

Converts raw CEX indicators into meaningful CEX metrics.
During analysis, it uses multiple submodules to extract higher-dimensional metrics.

   1. Receives “which CEX indicators to analyze” as settings and implements them as submodules.
   A task loop that implements arbitrary settings may also be added as a supporting component.

   2. Based on those settings, it receives real-time CEX data, processes it, and passes it to the interpreter.

5. Interpreter (not implemented):
(dxAn, cxAn) → {interpreter} → (actor)

A module that interprets indicators and converts them into commands.

   1. Stores “scripts (= user-input code)” that react to the indicators sent by dxAn and cxAn.
   Even when receiving the same indicators, the interpreter runs separately depending on each individual script.

   2. Ultimately, it takes the following as input:

      1. Code = static user script
      2. Record data = user portfolio
      3. Value input = indicators from dxAn and cxAn

      These become the code and environment of the interpreter.

      From this, it derives:

      1. Operand = what the command targets, such as a user, token, or exchange
      2. Opcode = what action to perform, such as buy, sell, or wait

      A command is composed of an operand and an opcode.

   3. Separately, in this case, the first-class entities of the language need to be carefully defined.

      1. I have an intuitive sense of the direction, but it is difficult to express precisely.
      2. The idea is to include indicator values, user portfolios, and similar concepts as first-class entities and base operators.
      Just as JavaScript supports structural sharing at the language level, this language should support value calculations that reflect spread arbitrage or interest rates at the language level.
      3. The runtime also needs to be carefully designed.
      4. Personally, I am also considering treating AI models themselves as first-class entities.
      Example: `m = NewModel("buy and pray"); m.Start()`
      5. Also, internally, I want to maintain time-series data using a TSDB and make “time-series-based structures” into first-class structures.
      6. For example:

      ```go
      type Delta struct {
          timeWindow time.Second
          metricKind MetricKind
          ptr        *[]MetricData
      }
      ```

      If this were treated as a primitive type like a Go slice and could be created using `%%`, then defining:

      ```go
      valanceDelta := %(valanceMetric)%
      ```

      would allow `print(valanceDelta)` to output the change over the last 1 second every time it is printed. In other words, time-series analysis would be handled automatically.

      Adding differential operators could also be useful.

      7. Personally, for time series or differentiation, I want all primitive value types handled by the language, such as balance, token, and so on, to satisfy a certain “differentiable” interface. Then these values could be easily derived through the language’s first-class operations.

      Example:

      ```go
      type Delatable interface {
          epsilon() time.Microsecond
          Divive() int
          Window() []int
      }
      ```

      If something like `~~` corresponds to a window operation, then something like:

      ```go
      for ~~valanceDelta range := valanceDelta
      ```

      could make it possible to iterate over the window of delta values each time.

6. Actor (not implemented):
(interpreter) → {actor} → (api call)

   1. Procedurally executes the “declarative commands” provided by the interpreter.
   2. Performs real-time evaluation, handles errors for impossible trades, logs activity, and monitors execution.
   3. In this module, performance and traditional error handling are likely to be important.

### shared

1. gface / workflow:
Provides high-level interfaces and higher-order functions for each module.
Can be used to enforce specific conventions.

2. domain:
Defines shared domain concepts.

3. meter:
Performs meter-based instrumentation.
Each meter appropriately wraps Prometheus while controlling performance overhead.

4. chaintimer:
Controls chain event time at a low level.
Inspired by the way cells operate.

5. dblib:
Implements various reusable or foundational database components.

6. kafka / eventub:
Provides event communication mechanisms.
