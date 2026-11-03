//! The `edbt` binary.

fn main() {
    let code = match edbt_cli::run_cli(std::env::args_os()) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            1
        }
    };
    std::process::exit(code);
}
