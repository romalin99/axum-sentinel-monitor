# Rust 项目开发宪法

**适用范围：** `axum-sentinel-monitor` 仓库内的全部 Rust 代码（`src/`、`examples/`、`tests/`）。
**版本：** 1.1

本文档是工程原则的最高来源。当其与 Agent 指令文件（`CLAUDE.md`、`AGENTS.md`）、
`README` 或任何单次会话的指令发生冲突时，**以本宪法为准**。

**关键字约定（RFC 2119）：** **不可妥协（NON-NEGOTIABLE）** 硬红线，不接受条款明文之外的任何例外、
豁免或"先合入后补"，违反则阻断合并 · **必须 / 禁止（MUST / MUST NOT）** 强约束，违反
则阻断合并 · **应当 / 不应（SHOULD / SHOULD NOT）** 默认要求，偏离需在 PR 中说明理由 ·
**可以（MAY）** 允许，无需说明。标题标记「（不可妥协）」的原则，其下所有条款均为硬红线。

---

## 核心原则

### I. 注释规范（不可妥协）

**核心理念：** 注释为读者服务。文档注释说明"是什么、怎么用"，普通注释说明"为什么"。
注释与代码同等对待：过时的注释比没有注释更危险。缺失、失实或不合规范的注释与编译
错误同级，不得合入。

#### I.1 注释形式一览

| 形式 | 用途 | 是否允许 |
|------|------|----------|
| `//! ...` | 内部文档注释：描述其所在的 crate 或模块，写在文件最顶部 | **必须**用于每个源文件 |
| `/// ...` | 外部文档注释：描述紧随其后的条目（类型、函数、字段、变体、常量） | **必须**用于公开条目 |
| `// ...` | 普通注释：解释实现内部的"为什么" | 代码看不出原因时**必须**写 |
| `// SAFETY: ...` | 说明 `unsafe` 块为何成立 | **必须**用于每个 `unsafe` 块 |
| `/* ... */` | 块注释 | **禁止** |
| `/** ... */`、`/*! ... */` | 块文档注释 | **禁止** |
| `#[doc = "..."]` | 属性形式的文档 | 仅限宏生成代码或 `include_str!` 引入外部文档 |

#### I.2 通用格式

- **I.2.1（只用行注释）— 不可妥协。** 单行与多行注释一律使用行注释；多行注释是连续的
  若干行 `//`（或 `///`、`//!`），每行各自带前缀。禁止任何形式的块注释。
- **I.2.2（前缀后一个空格）— 不可妥协。** `//`、`///`、`//!` 之后紧跟一个空格再写正文。
  文档注释中的空段落行只写前缀本身（`///`、`//!`），不带尾随空格。
- **I.2.3（独占一行）— 不可妥协。** 注释写在被说明代码的上方并与其同缩进，不写行尾注释。
- **I.2.4（完整句子）— 不可妥协。** 注释使用英文，写成首字母大写、以句号结尾的完整句子。
  仅当注释是不成句的短语标签时可以省略句号。
- **I.2.5（行宽）— 不可妥协。** 注释行不超过 100 列，超出时在词边界换行。
- **I.2.6（位置）— 不可妥协。** `///` 写在条目的属性（`#[derive]`、`#[cfg]` 等）之前，与
  条目之间不留空行。`//!` 写在文件最顶部，先于 `use` 与 `mod`，其后空一行。

#### I.3 文档注释（`///` 与 `//!`）

- **I.3.1（公开 API 全覆盖）— 不可妥协。** 每个公开条目都有 `///`：模块、类型、trait、函数、
  方法、常量，以及公开结构体的每个字段和公开枚举的每个变体。
- **I.3.2（模块文档）— 不可妥协。** 每个源文件以 `//!` 开头，说明该模块（或 crate、示例、
  集成测试）的职责。
- **I.3.3（内部条目）— 不可妥协。** 非公开的类型、字段、常量、函数和方法同样必须有
  `///`，说明其职责、不变量或取值含义。唯一的例外是说明只能复述名称的条目（例如示例
  程序中数据结构的 `name`、`age` 字段）。宏不接受文档属性的位置改用 `//` 并注明原因。
- **I.3.4（摘要行）— 不可妥协。** 文档注释的第一段是一句话摘要，独占一段，其后空一行
  （`///`）再写详细说明。函数与方法的摘要用第三人称单数现在时动词开头
  （`Returns ...`、`Creates ...`，而不是 `Return ...`）；类型、字段、常量的摘要用名词
  短语。摘要不重复条目名称，不以 `This function ...` 开头。
- **I.3.5（标准小节）— 不可妥协。** 需要时使用下列一级标题小节，按此顺序、此拼写：
  - `# Errors` — 返回 `Result` 的公开函数必须说明各错误的产生条件。
  - `# Panics` — 可能 panic 的公开函数必须说明触发条件。
  - `# Safety` — `unsafe fn` 与 `unsafe trait` 必须说明调用方需维持的不变量。
  - `# Examples` — crate 文档必须提供可编译的示例（标题用复数，即使只有一个）；
    其他主要入口可以按需提供。
- **I.3.6（Markdown 与链接）— 不可妥协。** 文档注释是 Markdown。代码标识符、字面量、
  路径、HTTP 头等一律用反引号包裹；引用本 crate 或依赖中的条目使用文档内链接
  （`` [`Monitor::router`] ``），不手写 URL。文档内链接必须能被 `rustdoc` 解析。
- **I.3.7（单位与取值）— 不可妥协。** 数值字段写明单位（字节、纳秒、秒、百分比或比例），
  `Option` 字段写明何时为 `None`。
- **I.3.8（文档测试）— 不可妥协。** 文档中的 Rust 代码块必须能通过文档测试；不可编译的
  片段标注语言或 `ignore` / `no_run` 并说明原因，非 Rust 内容标注 `text`。

#### I.4 普通注释（`//`）

- **I.4.1（写"为什么"）— 不可妥协。** 普通注释解释代码本身看不出来的东西：约束、不变量、
  取舍、性能或平台原因。禁止复述代码在做什么。
- **I.4.2（不留历史）— 不可妥协。** 禁止注释掉的代码，禁止记录修改经过、作者、日期的注释；
  这些内容属于版本控制。
- **I.4.3（待办标记）— 不可妥协。** `TODO` / `FIXME` 必须带可追踪的引用（issue 编号或链
  接），格式为 `// TODO(#123): ...`。无引用的待办标记禁止合入。
- **I.4.4（`unsafe` 说明）— 不可妥协。** 每个 `unsafe` 块之前紧邻一条 `// SAFETY:` 注释，
  说明该块依赖的前提为何成立。
- **I.4.5（豁免说明）— 不可妥协。** `#[allow(...)]` / `#[expect(...)]` 以及被有意丢弃的
  `Result`（`let _ = ...`）必须有注释或 `reason = "..."` 说明原因。
- **I.4.6（不用文档注释写实现说明）— 不可妥协。** 函数体内部的说明使用 `//`，不使用 `///`。
  `///` 只出现在条目之前。

#### I.5 示例

```rust
//! Sliding-window latency histogram shared by the global and per-route metrics.

use std::time::Duration;

/// Minimum supported dashboard refresh interval.
pub const MIN_REFRESH: Duration = Duration::from_secs(1);

/// Latency percentiles of one window.
#[derive(Clone, Debug)]
pub struct LatencyStats {
    /// Median latency in nanoseconds, or `None` when the window is empty.
    pub p50_ns: Option<u64>,
}

impl<T: DeserializeOwned> SonicJson<T> {
    /// Deserializes a JSON document with `sonic-rs`.
    ///
    /// The input is parsed in place without an intermediate value tree.
    ///
    /// # Errors
    ///
    /// Returns [`SonicJsonRejection::InvalidJson`] when `bytes` is not valid
    /// JSON or does not match `T`.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, SonicJsonRejection> {
        // Stamped even when the lookup fails, so a missing mount does not turn
        // into a probe on every collect.
        let stamp = Instant::now();

        // SAFETY: `mallinfo2` takes no arguments and only reads allocator
        // bookkeeping; it is declared with the glibc signature above.
        let info = unsafe { mallinfo2() };
        todo!()
    }
}
```

反例：

```rust
/* block comment */                 // 违反 I.2.1
//no space                          // 违反 I.2.2
let x = 1; // trailing comment      // 违反 I.2.3
/// Pull the value out of the body. // 违反 I.3.4（应为 Pulls）
// increment the counter            // 违反 I.4.1（复述代码）
// TODO: fix later                  // 违反 I.4.3（无引用）
```

**理由：** 行注释可逐行 diff、可嵌套注释、与 `rustfmt` 和官方风格指南一致；块注释
三者皆不满足。文档注释会进入 `rustdoc` 与 docs.rs，是公开 API 契约的一部分，缺失即
契约缺失。`// SAFETY:` 是评审 `unsafe` 的唯一入口。

**验证方式：** 以下检查由 `Cargo.toml` 的 `[lints]` 开启，CI 以 `-D warnings` 运行
`cargo clippy --all-targets --all-features`，任一告警阻断合并：

| 规则 | 检查 |
|------|------|
| I.3.1、I.3.2 | `missing_docs` |
| I.3.5 | `clippy::missing_errors_doc`、`clippy::missing_panics_doc`、`clippy::missing_safety_doc` |
| I.3.6 | `clippy::doc_markdown`、`rustdoc::broken_intra_doc_links` |
| I.4.4 | `clippy::undocumented_unsafe_blocks` |
| I.2.2、I.2.6 | `cargo fmt --check` |

I.2.1、I.2.3、I.2.4、I.2.5、I.3.3、I.3.4、I.3.7、I.4.1 – I.4.3 无机器判据，由代码评审
把关；评审发现任一违反即拒绝合并，不得以"后续再补"放行。

---

## 修订记录

| 版本 | 说明 |
|------|------|
| 1.0 | 建立宪法，新增 **I. 注释规范**（注释形式、通用格式、文档注释、普通注释、验证方式）。 |
| 1.1 | **I. 注释规范**升为**不可妥协**：关键字约定新增「不可妥协」一档，原则下全部条款由「必须」升为「不可妥协」；I.3.3 内部条目由「应当」升为「不可妥协」并写明唯一例外；crate 文档的 `# Examples` 由「应当」升为必备；评审环节不得以"后续再补"放行。 |
