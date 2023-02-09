use std::process::{Command, exit};

fn main() {
    let version = Command::new("git")
        .args(&["describe", "--tags", "--always"])
        .output()
        .unwrap_or_else(|_| {
            println!("cargo:warning=VERSION=Unknown");
            println!("cargo:rustc-env=VERSION=Unknown");
            exit(0);
        });

    let version = String::from_utf8(version.stdout)
        .unwrap()
        .trim()
        .to_owned();

    println!("cargo:rustc-env=VERSION={}", version);
}