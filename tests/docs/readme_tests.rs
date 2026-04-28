/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! README consistency checks for `qubit-rayon-batch`.

const CARGO_TOML: &str = include_str!("../../Cargo.toml");
const README_EN: &str = include_str!("../../README.md");
const README_ZH: &str = include_str!("../../README.zh_CN.md");

#[test]
/// Ensures README dependency snippets stay in sync with Cargo.toml.
fn test_readme_dependency_version_matches_cargo_toml() {
    let cargo_version =
        extract_package_version(CARGO_TOML).expect("Failed to extract version from Cargo.toml");
    let readme_en_version = extract_readme_dependency_version(README_EN)
        .expect("Failed to extract version from README.md");
    let readme_zh_version = extract_readme_dependency_version(README_ZH)
        .expect("Failed to extract version from README.zh_CN.md");
    assert_eq!(readme_en_version, cargo_version);
    assert_eq!(readme_zh_version, cargo_version);
}

#[test]
/// Ensures both README files document the current executor type.
fn test_readme_mentions_current_executor_type() {
    assert!(README_EN.contains("RayonBatchExecutor"));
    assert!(README_ZH.contains("RayonBatchExecutor"));
}

/// Extracts the first package version entry from Cargo.toml content.
fn extract_package_version(content: &str) -> Option<&str> {
    for line in content.lines() {
        if let Some(value) = line.strip_prefix("version = \"") {
            return value.strip_suffix('"');
        }
    }
    None
}

/// Extracts the `qubit-rayon-batch` dependency version from a README file.
fn extract_readme_dependency_version(content: &str) -> Option<&str> {
    for line in content.lines() {
        if let Some(value) = line.trim().strip_prefix("qubit-rayon-batch = \"") {
            return value.strip_suffix('"');
        }
    }
    None
}
