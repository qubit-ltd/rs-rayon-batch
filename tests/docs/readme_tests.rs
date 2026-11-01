// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! README consistency checks for `qubit-rayon-batch`.

const CARGO_TOML: &str = include_str!("../../Cargo.toml");
const README_EN: &str = include_str!("../../README.md");
const README_ZH: &str = include_str!("../../README.zh_CN.md");
const USER_GUIDE_EN: &str = include_str!("../../doc/user_guide.md");
const USER_GUIDE_ZH: &str = include_str!("../../doc/user_guide.zh_CN.md");

/// Ensures README dependency snippets stay in sync with Cargo.toml.
#[test]
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
        concat!(
            "README qubit-rayon-batch ({:?}) should match package version ",
            "{:?} (exact or minor line, e.g. 0.4 vs 0.4.1)"
        ),
        readme_en_rayon,
        package_version,
    );

    assert_eq!(readme_en_batch, cargo_qubit_batch);
    assert_eq!(readme_zh_batch, cargo_qubit_batch);
}

/// Ensures both user guides declare the current release and install version.
#[test]
fn test_user_guide_release_matches_cargo_toml() {
    let package = extract_package_version(CARGO_TOML).expect("Cargo package version must exist");
    let en = extract_guide_release(USER_GUIDE_EN).expect("English guide release must exist");
    let zh = extract_guide_release(USER_GUIDE_ZH).expect("Chinese guide release must exist");
    assert_eq!(en, zh, "guide release declarations must agree");
    for (name, guide) in [("English", USER_GUIDE_EN), ("Chinese", USER_GUIDE_ZH)] {
        let release = extract_guide_release(guide).expect("guide release must exist");
        assert!(
            readme_version_documents_same_release(release, package),
            "{name} guide release {release} must match package {package}"
        );
        let dependency = extract_readme_qubit_rayon_batch_version(guide).expect("guide dependency snippet must exist");
        assert!(
            readme_version_documents_same_release(dependency, package),
            "{name} guide dependency {dependency} must match package {package}"
        );
    }
}

/// Ensures both README files document the current executor type.
#[test]
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
fn extract_readme_qubit_rayon_batch_version(content: &str) -> Option<&str> {
    for line in content.lines() {
        if let Some(value) = line.trim().strip_prefix("qubit-rayon-batch = \"") {
            return value.strip_suffix('"');
        }
    }
    None
}

/// Extracts the release declared in the guide's applicability statement.
fn extract_guide_release(content: &str) -> Option<&str> {
    let marker = "`qubit-rayon-batch` ";
    let line = content.lines().find(|line| line.contains(marker))?;
    line.split_once(marker)?.1.split_whitespace().next()
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

/// Reads a dependency version from either a string or inline-table entry.
fn extract_cargo_dependency_version<'a>(content: &'a str, dep_name: &str) -> Option<&'a str> {
    let prefix = format!("{dep_name} = ");
    for line in content.lines() {
        let line = line.trim();
        if let Some(value) = line.strip_prefix(&prefix) {
            if let Some(version) = value.strip_prefix('"').and_then(|value| value.strip_suffix('"')) {
                return Some(version);
            }
            for field in value.strip_prefix('{')?.strip_suffix('}')?.split(',') {
                if let Some(version) = field
                    .trim()
                    .strip_prefix("version = \"")
                    .and_then(|value| value.strip_suffix('"'))
                {
                    return Some(version);
                }
            }
        }
    }
    None
}

/// README may document a minor line (`0.4`) while the crate uses a patch
/// (`0.4.1`).
fn readme_version_documents_same_release(readme: &str, package: &str) -> bool {
    readme == package || package.starts_with(&format!("{readme}."))
}
