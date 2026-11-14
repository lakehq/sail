use std::fs;
use std::path::Path;

/// Returns the number of bytes of memory that the process can use: the memory limit
/// of its control groups if they have one that is below the physical memory of the
/// machine, and the physical memory of the machine otherwise.
/// Returns [`None`] if neither is known.
pub(super) fn available_memory() -> Option<u64> {
    let limit = fs::read_to_string("/proc/self/cgroup")
        .ok()
        .and_then(|groups| cgroup_memory_limit(&groups, |path| fs::read_to_string(path).ok()));
    [limit, physical_memory()].into_iter().flatten().min()
}

/// Returns the smallest memory limit of the control groups of a process, given the
/// content of its `/proc/<pid>/cgroup` file and a function that returns the content
/// of a file that can be read.
fn cgroup_memory_limit(groups: &str, read: impl Fn(&Path) -> Option<String>) -> Option<u64> {
    groups
        .lines()
        .filter_map(|line| {
            // Each line is `<hierarchy>:<controllers>:<path>`, and the list of
            // controllers is empty for the single hierarchy of cgroup v2.
            let mut fields = line.splitn(3, ':').skip(1);
            let (controllers, path) = (fields.next()?, fields.next()?);
            let (root, file) = if controllers.is_empty() {
                ("/sys/fs/cgroup", "memory.max")
            } else if controllers
                .split(',')
                .any(|controller| controller == "memory")
            {
                ("/sys/fs/cgroup/memory", "memory.limit_in_bytes")
            } else {
                return None;
            };
            // The limit of a group applies to the groups inside it as well. A group
            // without a limit has no such file, the text `max` (cgroup v2), or a
            // number that is larger than any amount of memory (cgroup v1). Inside
            // a container the root is the group of the container, whatever the path.
            Path::new(path)
                .ancestors()
                .filter_map(|group| {
                    let group = Path::new(root).join(group.strip_prefix("/").ok()?);
                    read(&group.join(file))?.trim().parse::<u64>().ok()
                })
                .min()
        })
        .min()
}

/// Returns the physical memory of the machine in bytes.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn physical_memory() -> Option<u64> {
    // SAFETY: `sysconf` only takes and returns numbers.
    let (pages, page_size) = unsafe {
        (
            libc::sysconf(libc::_SC_PHYS_PAGES),
            libc::sysconf(libc::_SC_PAGESIZE),
        )
    };
    // A value that is not known is negative.
    u64::try_from(pages)
        .ok()?
        .checked_mul(u64::try_from(page_size).ok()?)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn physical_memory() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    /// Returns the memory limit for the content of `/proc/<pid>/cgroup` and the
    /// files that exist, as pairs of a path and the content.
    fn limit(groups: &str, files: &[(&str, &str)]) -> Option<u64> {
        let files = files
            .iter()
            .map(|(path, content)| (Path::new(path), *content))
            .collect::<HashMap<_, _>>();
        cgroup_memory_limit(groups, |path| files.get(path).copied().map(String::from))
    }

    #[test]
    fn cgroup_v2_limit_is_the_smallest_of_the_group_and_its_ancestors() {
        let groups = "0::/kubepods.slice/pod.slice/container.scope\n";
        let leaf = "/sys/fs/cgroup/kubepods.slice/pod.slice/container.scope/memory.max";
        let parent = "/sys/fs/cgroup/kubepods.slice/pod.slice/memory.max";
        let top = "/sys/fs/cgroup/kubepods.slice/memory.max";
        // The root group of the machine has no such file.
        let files = [(leaf, "max\n"), (parent, "8589934592\n"), (top, "max\n")];
        assert_eq!(limit(groups, &files), Some(8 << 30));
        let files = [(leaf, "1073741824\n"), (parent, "8589934592\n")];
        assert_eq!(limit(groups, &files), Some(1 << 30));
        let files = [(leaf, "8589934592\n"), (top, "1073741824\n")];
        assert_eq!(limit(groups, &files), Some(1 << 30));
        let files = [(leaf, "max\n"), (parent, "max\n"), (top, "max\n")];
        assert_eq!(limit(groups, &files), None);
        // Inside a container, the group of the container is the root.
        let files = [("/sys/fs/cgroup/memory.max", "4294967296\n")];
        assert_eq!(limit("0::/\n", &files), Some(4 << 30));
        assert_eq!(limit("0::/../other.scope\n", &files), Some(4 << 30));
    }

    #[test]
    fn cgroup_v1_limit_is_the_smallest_of_the_group_and_its_ancestors() {
        let groups = "12:memory:/docker/5f1e\n\
                      11:cpu,cpuacct:/docker/5f1e\n\
                      1:name=systemd:/docker/5f1e\n";
        let leaf = "/sys/fs/cgroup/memory/docker/5f1e/memory.limit_in_bytes";
        let parent = "/sys/fs/cgroup/memory/docker/memory.limit_in_bytes";
        let root = "/sys/fs/cgroup/memory/memory.limit_in_bytes";
        let unlimited = "9223372036854771712\n";
        let files = [
            (leaf, "2147483648\n"),
            (parent, unlimited),
            (root, unlimited),
        ];
        assert_eq!(limit(groups, &files), Some(2 << 30));
        let files = [
            (leaf, unlimited),
            (parent, "1073741824\n"),
            (root, unlimited),
        ];
        assert_eq!(limit(groups, &files), Some(1 << 30));
        // Inside a container, the group of the container is the root.
        let files = [(root, "2147483648\n")];
        assert_eq!(limit(groups, &files), Some(2 << 30));
        // The number of a group without a limit is larger than any amount of memory.
        let files = [(leaf, unlimited), (parent, unlimited), (root, unlimited)];
        assert!(limit(groups, &files).is_some_and(|limit| limit > 1 << 60));
        // The memory controller can share its hierarchy with other controllers.
        let files = [(root, "2147483648\n")];
        assert_eq!(limit("3:cpu,memory:/\n", &files), Some(2 << 30));
    }

    #[test]
    fn cgroup_limit_is_the_smallest_of_all_hierarchies() {
        let groups = "5:memory:/a\n0::/b\n";
        let v1 = "/sys/fs/cgroup/memory/a/memory.limit_in_bytes";
        let v2 = "/sys/fs/cgroup/b/memory.max";
        assert_eq!(limit(groups, &[(v1, "100\n"), (v2, "200\n")]), Some(100));
        assert_eq!(limit(groups, &[(v1, "200\n"), (v2, "100\n")]), Some(100));
        assert_eq!(limit(groups, &[(v1, "200\n")]), Some(200));
    }

    #[test]
    fn cgroup_limit_is_unknown_without_files_that_have_a_number() {
        let v1 = "/sys/fs/cgroup/memory/a/memory.limit_in_bytes";
        let v2 = "/sys/fs/cgroup/a/memory.max";
        let files = [(v1, "100\n"), (v2, "200\n")];
        assert_eq!(limit("", &files), None);
        assert_eq!(limit("garbage\n", &files), None);
        assert_eq!(limit("0:\n", &files), None);
        assert_eq!(limit("0::a\n", &files), None);
        assert_eq!(limit("4:cpu:/a\n2:name=memory:/a\n", &files), None);
        assert_eq!(limit("0::/a\n5:memory:/a\n", &[]), None);
        let files = [(v1, "\n"), (v2, "-1\n")];
        assert_eq!(limit("0::/a\n5:memory:/a\n", &files), None);
        let files = [(v1, "100 bytes\n"), (v2, "1e9\n")];
        assert_eq!(limit("0::/a\n5:memory:/a\n", &files), None);
    }
}
