# contrib

## strings

given:

```go
func generateValidationErrorMessage(err error) string {
    errors := []string{}

    for _, err := range err.(validator.ValidationErrors) {
        var b strings.Builder
        b.WriteString(fmt.Sprintf("[%s] failed validation [%s]", err.Field(), err.ActualTag()))
        if err.Param() != "" {
            b.WriteString(fmt.Sprintf("[%s]", err.Param()))
        }
        errors = append(errors, b.String())
    }

    return strings.Join(errors, "; ")
}
```

we can refactor as:

```go
func generateValidationErrorMessage(err error) string {
    var b strings.Builder

    for i, err := range err.(validator.ValidationErrors) {
        if i > 0 {
            fmt.Fprint(&b, "; ")
        } 
        fmt.Fprintf(&b, "[%s] failed validation [%s]", err.Field(), err.ActualTag())
        if err.Param() != "" {
            fmt.Fprintf(&b, "[%s]", err.Param())
        }
    }

    return b.String()
}
```

---

- [Structured concurrency & Go](https://rednafi.com/go/structured-concurrency/)
- [Advanced Go concurrency](https://encore.dev/blog/advanced-go-concurrency)
- [How to gracefully close channels](https://go101.org/article/channel-closing.html)
- [Zero copy](https://goperf.dev/01-common-patterns/zero-copy/)
- [Alternative to using strings.Builder in conjunction with fmt.Sprintf](https://stackoverflow.com/a/74895891)
- [Testing os/exec.Command](https://npf.io/2015/06/testing-exec-command/)
- [How to mock *exec.Cmd/exec.Command()?](https://stackoverflow.com/a/71166114)
- [test subprocesses](https://rednafi.com/go/test-subprocesses/)
