# check Command

For detailed usage and examples, see the [Check User Guide](../guide/check.md).

## Quick Reference

```bash
gpio check --help
```

This will show all available subcommands and options.

## Subcommands

- `check all` - Run all validation checks
- `check spatial` - Check spatial ordering and filter pushdown readiness
- `check compression` - Validate compression settings
- `check bbox` - Verify bbox structure and metadata
- `check row-group` - Check row group optimization
- `check optimization` - Combined spatial query optimization score (5 factors)
- `check spec` - Validate against GeoParquet specification
- `check stac` - Validate STAC Item or Collection JSON
