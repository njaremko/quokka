load("@prelude//rust:rust_binary.bzl", "rust_test_impl")
load("@prelude//decls:rust_rules.bzl", "rust_test")

def _cached_rust_test_impl(ctx: AnalysisContext) -> list[Provider]:
    providers = rust_test_impl(ctx)
    new_providers = []
    test_info = None
    
    for p in providers:
        if type(p) == "ExternalRunnerTestInfo":
            test_info = p
            break

    if test_info != None:
        new_test_info = ExternalRunnerTestInfo(
            type = getattr(test_info, "test_type", "rust"),
            command = test_info.command,
            env = test_info.env,
            labels = test_info.labels,
            contacts = test_info.contacts,
            default_executor = test_info.default_executor,
            executor_overrides = test_info.executor_overrides,
            run_from_project_root = test_info.run_from_project_root,
            use_project_relative_paths = test_info.use_project_relative_paths,
            supports_test_execution_caching = True,  # <-- Enable Caching
            local_resources = getattr(test_info, "local_resources", None),
            required_local_resources = getattr(test_info, "required_local_resources", None),
            worker = getattr(test_info, "worker", None),
        )
        for p in providers:
            if type(p) == "ExternalRunnerTestInfo":
                new_providers.append(new_test_info)
            else:
                new_providers.append(p)
    else:
        new_providers = providers
        
    return new_providers

cached_rust_test = rule(
    impl = _cached_rust_test_impl,
    attrs = rust_test.attrs,
    uses_plugins = rust_test.uses_plugins,
    supports_incoming_transition = getattr(rust_test, "supports_incoming_transition", True),
)
