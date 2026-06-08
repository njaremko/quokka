load(
    "@prelude//cxx:cxx_toolchain_types.bzl",
    "BinaryUtilitiesInfo",
    "CCompilerInfo",
    "CvtresCompilerInfo",
    "CxxCompilerInfo",
    "CxxInternalTools",
    "CxxPlatformInfo",
    "CxxToolchainInfo",
    "DepTrackingMode",
    "LinkerInfo",
    "LinkerType",
    "PicBehavior",
    "RcCompilerInfo",
    "RuntimeDependencyHandling",
    "ShlibInterfacesMode",
)
load("@prelude//cxx:headers.bzl", "HeaderMode")
load("@prelude//cxx:linker.bzl", "is_pdb_generated")
load("@prelude//decls:common.bzl", "buck")
load("@prelude//linking:link_info.bzl", "LinkOrdering", "LinkStyle")
load("@prelude//linking:lto.bzl", "LtoMode")
load("@prelude//os_lookup:defs.bzl", "Os", "OsLookup")

def _quokka_nix_cxx_toolchain_impl(ctx):
    os = ctx.attrs._target_os_type[OsLookup].os
    arch_name = ctx.attrs._target_os_type[OsLookup].cpu
    target_name = os.value
    if arch_name:
        target_name += "-" + arch_name

    compiler_type = ctx.attrs.compiler_type
    if os == Os("windows"):
        linker_type = LinkerType("windows")
        binary_extension = "exe"
        object_file_extension = "obj"
        static_library_extension = "lib"
        shared_library_name_default_prefix = ""
        shared_library_name_format = "{}.dll"
        shared_library_versioned_name_format = "{}.dll"
        pic_behavior = PicBehavior("not_supported")
    else:
        linker_type = LinkerType("darwin") if os == Os("macos") else LinkerType("gnu")
        binary_extension = ""
        object_file_extension = "o"
        static_library_extension = "a"
        shared_library_name_default_prefix = "lib"
        if os == Os("macos"):
            shared_library_name_format = "lib{}.dylib"
            shared_library_versioned_name_format = "lib{}.{}.dylib"
            pic_behavior = PicBehavior("always_enabled")
        else:
            shared_library_name_format = "{}.so"
            shared_library_versioned_name_format = "{}.so.{}"
            pic_behavior = PicBehavior("supported")

    additional_linker_flags = ["-fuse-ld=lld"] if os == Os("linux") else []
    link_locally = os == Os("macos") and not ctx.attrs.remote_execution
    cpp_dep_tracking_mode = DepTrackingMode("show_headers") if compiler_type in ("clang", "clang_cl", "clang_windows") else DepTrackingMode("makefile")
    assembler = ctx.attrs.assembler[RunInfo] if ctx.attrs.assembler else ctx.attrs.compiler[RunInfo]

    return [
        DefaultInfo(),
        CxxToolchainInfo(
            internal_tools = ctx.attrs.internal_tools[CxxInternalTools],
            linker_info = LinkerInfo(
                linker = ctx.attrs.linker[RunInfo],
                linker_flags = additional_linker_flags + ctx.attrs.link_flags,
                post_linker_flags = ctx.attrs.post_link_flags,
                archiver = ctx.attrs.archiver[RunInfo],
                archiver_type = ctx.attrs.archiver_type,
                archiver_supports_argfiles = os != Os("macos"),
                generate_linker_maps = False,
                lto_mode = LtoMode("none"),
                type = linker_type,
                link_binaries_locally = link_locally,
                link_libraries_locally = link_locally,
                archive_objects_locally = link_locally,
                use_archiver_flags = True,
                static_dep_runtime_ld_flags = [],
                static_pic_dep_runtime_ld_flags = [],
                shared_dep_runtime_ld_flags = [],
                independent_shlib_interface_linker_flags = [],
                shlib_interfaces = ShlibInterfacesMode("disabled"),
                link_style = LinkStyle(ctx.attrs.link_style),
                link_weight = 1,
                binary_extension = binary_extension,
                object_file_extension = object_file_extension,
                shared_library_name_default_prefix = shared_library_name_default_prefix,
                shared_library_name_format = shared_library_name_format,
                shared_library_versioned_name_format = shared_library_versioned_name_format,
                static_library_extension = static_library_extension,
                force_full_hybrid_if_capable = False,
                is_pdb_generated = is_pdb_generated(linker_type, ctx.attrs.link_flags),
                link_ordering = ctx.attrs.link_ordering,
            ),
            bolt_enabled = False,
            binary_utilities_info = BinaryUtilitiesInfo(
                nm = ctx.attrs.nm[RunInfo],
                objcopy = ctx.attrs.objcopy[RunInfo],
                objdump = ctx.attrs.objdump[RunInfo],
                ranlib = ctx.attrs.ranlib[RunInfo],
                strip = ctx.attrs.strip[RunInfo],
                dwp = None,
                bolt_msdk = None,
            ),
            cxx_compiler_info = CxxCompilerInfo(
                compiler = ctx.attrs.cxx_compiler[RunInfo],
                preprocessor_flags = [],
                compiler_flags = ctx.attrs.cxx_flags,
                compiler_type = compiler_type,
                supports_two_phase_compilation = ctx.attrs.supports_two_phase_compilation,
                supports_content_based_paths = ctx.attrs.supports_content_based_paths,
            ),
            c_compiler_info = CCompilerInfo(
                compiler = ctx.attrs.compiler[RunInfo],
                preprocessor_flags = [],
                compiler_flags = ctx.attrs.c_flags,
                compiler_type = compiler_type,
                supports_content_based_paths = ctx.attrs.supports_content_based_paths,
            ),
            as_compiler_info = CCompilerInfo(
                compiler = ctx.attrs.compiler[RunInfo],
                compiler_type = compiler_type,
                supports_content_based_paths = ctx.attrs.supports_content_based_paths,
            ),
            asm_compiler_info = CCompilerInfo(
                compiler = assembler,
                compiler_type = ctx.attrs.asm_compiler_type,
            ),
            cvtres_compiler_info = CvtresCompilerInfo(
                compiler = ctx.attrs.cvtres_compiler[RunInfo] if ctx.attrs.cvtres_compiler else None,
                preprocessor_flags = [],
                compiler_flags = ctx.attrs.cvtres_flags,
                compiler_type = compiler_type,
            ),
            rc_compiler_info = RcCompilerInfo(
                compiler = ctx.attrs.rc_compiler[RunInfo] if ctx.attrs.rc_compiler else None,
                preprocessor_flags = [],
                compiler_flags = ctx.attrs.rc_flags,
                compiler_type = compiler_type,
            ),
            header_mode = HeaderMode("symlink_tree_only"),
            cpp_dep_tracking_mode = cpp_dep_tracking_mode,
            pic_behavior = pic_behavior,
            llvm_link = ctx.attrs.llvm_link[RunInfo] if ctx.attrs.llvm_link else None,
            use_dep_files = True,
            runtime_dependency_handling = RuntimeDependencyHandling("no_symlink"),
        ),
        CxxPlatformInfo(name = target_name),
    ]

quokka_nix_cxx_toolchain = rule(
    impl = _quokka_nix_cxx_toolchain_impl,
    attrs = {
        "archiver": attrs.exec_dep(providers = [RunInfo]),
        "archiver_type": attrs.string(default = "gnu"),
        "assembler": attrs.option(attrs.exec_dep(providers = [RunInfo]), default = None),
        "asm_compiler_type": attrs.string(default = "clang"),
        "c_flags": attrs.list(attrs.arg(), default = []),
        "compiler": attrs.exec_dep(providers = [RunInfo]),
        "compiler_type": attrs.string(default = "clang"),
        "cvtres_compiler": attrs.option(attrs.exec_dep(providers = [RunInfo]), default = None),
        "cvtres_flags": attrs.list(attrs.arg(), default = []),
        "cxx_compiler": attrs.exec_dep(providers = [RunInfo]),
        "cxx_flags": attrs.list(attrs.arg(), default = []),
        "internal_tools": attrs.default_only(attrs.exec_dep(providers = [CxxInternalTools], default = "prelude//cxx/tools:internal_tools")),
        "link_flags": attrs.list(attrs.arg(), default = []),
        "link_ordering": attrs.option(attrs.enum(LinkOrdering.values()), default = None),
        "link_style": attrs.enum(LinkStyle.values(), default = "shared"),
        "linker": attrs.exec_dep(providers = [RunInfo]),
        "llvm_link": attrs.option(attrs.exec_dep(providers = [RunInfo]), default = None),
        "nm": attrs.exec_dep(providers = [RunInfo]),
        "objcopy": attrs.exec_dep(providers = [RunInfo]),
        "objdump": attrs.exec_dep(providers = [RunInfo]),
        "post_link_flags": attrs.list(attrs.arg(), default = []),
        "ranlib": attrs.exec_dep(providers = [RunInfo]),
        "rc_compiler": attrs.option(attrs.exec_dep(providers = [RunInfo]), default = None),
        "rc_flags": attrs.list(attrs.arg(), default = []),
        "remote_execution": attrs.bool(default = False),
        "strip": attrs.exec_dep(providers = [RunInfo]),
        "supports_content_based_paths": attrs.bool(default = False),
        "supports_two_phase_compilation": attrs.bool(default = False),
        "_target_os_type": buck.target_os_type_arg(),
    },
    is_toolchain_rule = True,
)
