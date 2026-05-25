/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! README consistency checks for `qubit-rayon-batch`.

const CARGO_TOML: &str = include_str!("../../Cargo.toml");
const README_EN: &str = include_str!("../../README.md");
const README_ZH: &str = include_str!("../../README.zh_CN.md");
const RAYON_BATCH_EXECUTOR: &str = include_str!("../../src/rayon_batch_executor.rs");

#[test]
/// Ensures README dependency snippets stay in sync with Cargo.toml.
fn test_readme_dependency_version_matches_cargo_toml() {
    let package_version = extract_package_version(CARGO_TOML).expect("Failed to extract version from Cargo.toml");
    let cargo_qubit_batch = extract_cargo_dependency_version(CARGO_TOML, "qubit-batch")
        .expect("Failed to extract qubit-batch from Cargo.toml");

    let readme_en_rayon = extract_readme_qubit_rayon_batch_version(README_EN)
        .expect("Failed to extract qubit-rayon-batch from README.md");
    let readme_zh_rayon = extract_readme_qubit_rayon_batch_version(README_ZH)
        .expect("Failed to extract qubit-rayon-batch from README.zh_CN.md");
    let readme_en_batch =
        extract_readme_qubit_batch_version(README_EN).expect("Failed to extract qubit-batch from README.md");
    let readme_zh_batch =
        extract_readme_qubit_batch_version(README_ZH).expect("Failed to extract qubit-batch from README.zh_CN.md");

    assert_eq!(
        readme_en_rayon, readme_zh_rayon,
        "README.md and README.zh_CN.md should document the same qubit-rayon-batch version"
    );
    assert!(
        readme_version_documents_same_release(readme_en_rayon, package_version),
        "README qubit-rayon-batch ({readme_en_rayon:?}) should match package version {package_version:?} (exact or minor line, e.g. 0.4 vs 0.4.1)"
    );

    assert_eq!(readme_en_batch, cargo_qubit_batch);
    assert_eq!(readme_zh_batch, cargo_qubit_batch);
}

#[test]
/// Ensures both README files document the current executor type.
fn test_readme_mentions_current_executor_type() {
    assert!(README_EN.contains("RayonBatchExecutor"));
    assert!(README_ZH.contains("RayonBatchExecutor"));
}

#[test]
/// Ensures Rayon progress reporting uses the shared scoped progress guard.
fn test_rayon_progress_reporting_uses_scoped_progress_guard() {
    assert!(RAYON_BATCH_EXECUTOR.contains("spawn_running_reporter"));
    assert!(!RAYON_BATCH_EXECUTOR.contains("RunningProgressLoop"));
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
fn extract_readme_qubit_rayon_batch_version(content: &str) -> Option<&str> {
    for line in content.lines() {
        if let Some(value) = line.trim().strip_prefix("qubit-rayon-batch = \"") {
            return value.strip_suffix('"');
        }
    }
    None
}

/// Extracts the `qubit-batch` dependency version from a README file.
fn extract_readme_qubit_batch_version(content: &str) -> Option<&str> {
    for line in content.lines() {
        if let Some(value) = line.trim().strip_prefix("qubit-batch = \"") {
            return value.strip_suffix('"');
        }
    }
    None
}

/// Reads `[dependencies] dep_name = "..."` from Cargo.toml (first match).
fn extract_cargo_dependency_version<'a>(content: &'a str, dep_name: &str) -> Option<&'a str> {
    let prefix = format!("{dep_name} = \"");
    for line in content.lines() {
        let line = line.trim();
        if let Some(value) = line.strip_prefix(&prefix) {
            return value.strip_suffix('"');
        }
    }
    None
}

/// README may document a minor line (`0.4`) while the crate uses a patch (`0.4.1`).
fn readme_version_documents_same_release(readme: &str, package: &str) -> bool {
    readme == package || package.starts_with(&format!("{readme}."))
}
