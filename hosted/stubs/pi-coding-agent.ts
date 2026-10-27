// Hosted Kanna doesn't run Pi: it executes its own tools in-process, which
// here would be inside the Durable Object rather than on the machine. The
// hosted build drops it from the pickers (`disabledProviders`) and replaces
// the SDK with this, so its module tree stays out of the bundle.
throw new Error("Pi isn't available in hosted Kanna")
