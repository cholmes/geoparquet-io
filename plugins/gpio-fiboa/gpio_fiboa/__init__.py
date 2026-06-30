"""gpio-fiboa: fiboa (Field Boundaries for Agriculture) plugin for gpio."""

__version__ = "0.1.0"


def __getattr__(name):
    if name == "improve_fiboa":
        from gpio_fiboa.improve import improve_fiboa

        return improve_fiboa
    if name == "describe_fiboa":
        from gpio_fiboa.describe import describe_fiboa

        return describe_fiboa
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["improve_fiboa", "describe_fiboa"]
