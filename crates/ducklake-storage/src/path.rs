//! Path utilities for DuckLake storage operations

use std::path::Path;

/// Utilities for working with storage paths
pub struct PathUtils;

impl PathUtils {
    /// Normalize a path by converting backslashes to forward slashes
    /// This ensures consistent paths across different operating systems
    pub fn normalize_path<P: AsRef<Path>>(path: P) -> String {
        path.as_ref().to_string_lossy().replace('\\', "/")
    }

    /// Join path components using forward slashes
    pub fn join_paths(base: &str, path: &str) -> String {
        if path.starts_with('/') {
            // Absolute path
            path.to_string()
        } else if base.ends_with('/') {
            format!("{}{}", base, path)
        } else {
            format!("{}/{}", base, path)
        }
    }

    /// Extract the directory portion of a path
    pub fn parent_path(path: &str) -> Option<String> {
        let path = Path::new(path);
        path.parent().map(|p| Self::normalize_path(p))
    }

    /// Extract the filename portion of a path
    pub fn file_name(path: &str) -> Option<String> {
        let path = Path::new(path);
        path.file_name()
            .map(|name| name.to_string_lossy().to_string())
    }

    /// Check if a path is absolute
    pub fn is_absolute(path: &str) -> bool {
        path.starts_with('/') || path.contains(':')
    }

    /// Convert a relative path to absolute based on a base path
    pub fn to_absolute(base: &str, path: &str) -> String {
        if Self::is_absolute(path) {
            path.to_string()
        } else {
            Self::join_paths(base, path)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_path() {
        assert_eq!(PathUtils::normalize_path("path\\to\\file"), "path/to/file");
        assert_eq!(PathUtils::normalize_path("path/to/file"), "path/to/file");
    }

    #[test]
    fn test_join_paths() {
        assert_eq!(PathUtils::join_paths("base", "file"), "base/file");
        assert_eq!(PathUtils::join_paths("base/", "file"), "base/file");
        assert_eq!(PathUtils::join_paths("base", "/absolute"), "/absolute");
    }

    #[test]
    fn test_parent_path() {
        assert_eq!(
            PathUtils::parent_path("path/to/file"),
            Some("path/to".to_string())
        );
        assert_eq!(PathUtils::parent_path("file"), Some("".to_string()));
    }

    #[test]
    fn test_file_name() {
        assert_eq!(
            PathUtils::file_name("path/to/file.txt"),
            Some("file.txt".to_string())
        );
        assert_eq!(
            PathUtils::file_name("file.txt"),
            Some("file.txt".to_string())
        );
    }

    #[test]
    fn test_is_absolute() {
        assert!(PathUtils::is_absolute("/absolute/path"));
        assert!(PathUtils::is_absolute("C:\\windows\\path"));
        assert!(!PathUtils::is_absolute("relative/path"));
    }

    #[test]
    fn test_to_absolute() {
        assert_eq!(
            PathUtils::to_absolute("/base", "relative"),
            "/base/relative"
        );
        assert_eq!(PathUtils::to_absolute("/base", "/absolute"), "/absolute");
    }
}
