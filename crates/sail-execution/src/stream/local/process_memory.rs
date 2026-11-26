// SPDX-License-Identifier: Apache-2.0

use std::fs;
use std::path::Path;

/// Returns the number of bytes of memory that the process can use: the memory limit
/// of its control groups if they have one that is below the physical memory of the
/// machine, and the physical memory of the machine otherwise.
/// Returns [`None`] if neither is known.
pub(super) fn available_memory() -> Option<u64> {
    let limit = fs::read_to_string("/proc/self/cgroup")
        .ok()
        .and_then(|groups| {
            let mounts = fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
            cgroup_memory_limit(&groups, &mounts, |path| fs::read_to_string(path).ok())
        });
    [limit, physical_memory()].into_iter().flatten().min()
}

/// Returns the smallest memory limit of the control groups of a process, given the
/// content of its `/proc/<pid>/cgroup` and `/proc/<pid>/mountinfo` files and a
/// function that returns the content of a file that can be read.
fn cgroup_memory_limit(
    groups: &str,
    mounts: &str,
    read: impl Fn(&Path) -> Option<String>,
) -> Option<u64> {
    let mounts = cgroup_mounts(mounts);
    let read = &read;
    groups
        .lines()
        .filter_map(|line| {
            // Each line is `<hierarchy>:<controllers>:<path>`, and the list of
            // controllers is empty for the single hierarchy of cgroup v2.
            let mut fields = line.splitn(3, ':').skip(1);
            let (controllers, path) = (fields.next()?, fields.next()?);
            let (unified, usual, file) = if controllers.is_empty() {
                (true, "/sys/fs/cgroup", "memory.max")
            } else if controllers
                .split(',')
                .any(|controller| controller == "memory")
            {
                (false, "/sys/fs/cgroup/memory", "memory.limit_in_bytes")
            } else {
                return None;
            };
            // The group is a directory below each place where its hierarchy is
            // mounted. A mount shows the hierarchy from one group downwards, which
            // is the group of the container inside a container, so the path of the
            // group is taken relative to that group. Without such a mount, the
            // hierarchy is looked for in its usual place.
            let path = Path::new(path);
            let mut places = mounts
                .iter()
                .filter(|mount| mount.unified == unified)
                .filter_map(|mount| Some((mount.point, path.strip_prefix(mount.root).ok()?)))
                .collect::<Vec<_>>();
            if places.is_empty() {
                places.push((Path::new(usual), path.strip_prefix("/").ok()?));
            }
            // The limit of a group applies to the groups inside it as well. A group
            // without a limit has no such file, the text `max` (cgroup v2), or a
            // number that is larger than any amount of memory (cgroup v1).
            places
                .into_iter()
                .flat_map(|(point, group)| {
                    group.ancestors().filter_map(move |group| {
                        let limit = read(&point.join(group).join(file))?;
                        limit.trim().parse::<u64>().ok()
                    })
                })
                .min()
        })
        .min()
}

/// A place where a hierarchy of control groups that can limit memory is mounted.
struct CgroupMount<'a> {
    /// Whether this is the single hierarchy of cgroup v2.
    unified: bool,
    /// The path of the group that the mount shows as its top directory.
    root: &'a Path,
    point: &'a Path,
}

/// Returns the mounts of the hierarchies of control groups that can limit memory,
/// given the content of a `/proc/<pid>/mountinfo` file.
fn cgroup_mounts(mounts: &str) -> Vec<CgroupMount<'_>> {
    mounts
        .lines()
        .filter_map(|line| {
            // Each line is `<id> <parent> <device> <root> <mount point> <options>
            // <more fields> - <type> <source> <options of the file system>`. The
            // controllers of a cgroup v1 hierarchy are options of the file system.
            // A path with a space is written with an escape and is not found here.
            let (mount, filesystem) = line.split_once(" - ")?;
            let mut mount = mount.split(' ').skip(3);
            let (root, point) = (mount.next()?, mount.next()?);
            let mut filesystem = filesystem.split(' ');
            let (kind, options) = (filesystem.next()?, filesystem.nth(1)?);
            let unified = match kind {
                "cgroup2" => true,
                "cgroup" if options.split(',').any(|option| option == "memory") => false,
                _ => return None,
            };
            Some(CgroupMount {
                unified,
                root: Path::new(root),
                point: Path::new(point),
            })
        })
        .collect()
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
    /// files that exist, as pairs of a path and the content, when the hierarchies
    /// are mounted as in the content of `/proc/<pid>/mountinfo`.
    fn limit_with_mounts(groups: &str, mounts: &str, files: &[(&str, &str)]) -> Option<u64> {
        let files = files
            .iter()
            .map(|(path, content)| (Path::new(path), *content))
            .collect::<HashMap<_, _>>();
        cgroup_memory_limit(groups, mounts, |path| {
            files.get(path).copied().map(String::from)
        })
    }

    /// Returns the memory limit when the mounts of the process are not known.
    fn limit(groups: &str, files: &[(&str, &str)]) -> Option<u64> {
        limit_with_mounts(groups, "", files)
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

    const V2_MOUNT: &str = "35 25 0:30 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime shared:9 \
                            - cgroup2 cgroup2 rw,nsdelegate,memory_recursiveprot\n";
    const V1_MOUNTS: &str = "41 34 0:36 / /sys/fs/cgroup/memory rw,nosuid,nodev,noexec,relatime \
                             shared:17 - cgroup cgroup rw,memory\n\
                             38 34 0:33 / /sys/fs/cgroup/cpu,cpuacct rw,nosuid,nodev,noexec,relatime \
                             shared:14 - cgroup cgroup rw,cpu,cpuacct\n\
                             35 34 0:30 / /sys/fs/cgroup/systemd rw,nosuid,nodev,noexec,relatime \
                             shared:11 - cgroup cgroup rw,xattr,name=systemd\n";

    #[test]
    fn cgroup_limit_is_found_where_the_hierarchy_is_usually_mounted() {
        let groups = "0::/kubepods.slice/pod.slice\n";
        let files = [
            (
                "/sys/fs/cgroup/kubepods.slice/pod.slice/memory.max",
                "max\n",
            ),
            ("/sys/fs/cgroup/kubepods.slice/memory.max", "8589934592\n"),
        ];
        assert_eq!(limit_with_mounts(groups, V2_MOUNT, &files), Some(8 << 30));
        let groups = "12:memory:/docker/5f1e\n11:cpu,cpuacct:/docker/5f1e\n";
        let files = [
            (
                "/sys/fs/cgroup/memory/docker/5f1e/memory.limit_in_bytes",
                "2147483648\n",
            ),
            // A hierarchy without the memory controller has no memory limit.
            (
                "/sys/fs/cgroup/cpu,cpuacct/docker/5f1e/memory.limit_in_bytes",
                "1\n",
            ),
        ];
        assert_eq!(limit_with_mounts(groups, V1_MOUNTS, &files), Some(2 << 30));
    }

    #[test]
    fn cgroup_limit_is_found_where_the_hierarchy_is_mounted_elsewhere() {
        // The memory controller of cgroup v1 shares a hierarchy that has its own place.
        let mounts = "30 24 0:26 / /cgroup/cpu_memory rw,relatime - cgroup cgroup rw,cpu,memory\n";
        let groups = "3:cpu,memory:/batch/job\n";
        let files = [
            (
                "/cgroup/cpu_memory/batch/job/memory.limit_in_bytes",
                "2147483648\n",
            ),
            (
                "/cgroup/cpu_memory/batch/memory.limit_in_bytes",
                "4294967296\n",
            ),
            // Nothing is read from the usual place.
            (
                "/sys/fs/cgroup/memory/batch/job/memory.limit_in_bytes",
                "1\n",
            ),
        ];
        assert_eq!(limit_with_mounts(groups, mounts, &files), Some(2 << 30));
        // The hierarchy of cgroup v2 is mounted next to the hierarchies of cgroup v1.
        let mounts = format!(
            "{V1_MOUNTS}36 34 0:31 / /sys/fs/cgroup/unified rw,nosuid,nodev,noexec,relatime \
             shared:10 - cgroup2 cgroup2 rw,nsdelegate\n"
        );
        let groups = "5:memory:/user.slice\n0::/user.slice/session.scope\n";
        let files = [
            (
                "/sys/fs/cgroup/unified/user.slice/memory.max",
                "1073741824\n",
            ),
            (
                "/sys/fs/cgroup/memory/user.slice/memory.limit_in_bytes",
                "2147483648\n",
            ),
        ];
        assert_eq!(limit_with_mounts(groups, &mounts, &files), Some(1 << 30));
        assert_eq!(
            limit_with_mounts(groups, &mounts, &files[1..]),
            Some(2 << 30)
        );
    }

    #[test]
    fn cgroup_limit_is_found_when_the_mount_starts_at_the_group_of_the_container() {
        let mounts = "512 500 0:36 /docker/5f1e /sys/fs/cgroup/memory ro,nosuid,nodev,noexec,relatime \
                      master:17 - cgroup cgroup rw,memory\n";
        let top = "/sys/fs/cgroup/memory/memory.limit_in_bytes";
        let inner = "/sys/fs/cgroup/memory/inner/memory.limit_in_bytes";
        let files = [(top, "2147483648\n"), (inner, "1073741824\n")];
        assert_eq!(
            limit_with_mounts("12:memory:/docker/5f1e\n", mounts, &files),
            Some(2 << 30)
        );
        // A group inside the container is a directory below the mount point.
        assert_eq!(
            limit_with_mounts("12:memory:/docker/5f1e/inner\n", mounts, &files),
            Some(1 << 30)
        );
        let mounts = "512 500 0:30 /pod.slice/container.scope /mnt/cgroup ro,relatime - cgroup2 cgroup2 rw\n";
        let files = [
            ("/mnt/cgroup/memory.max", "4294967296\n"),
            ("/mnt/cgroup/inner/memory.max", "max\n"),
        ];
        assert_eq!(
            limit_with_mounts("0::/pod.slice/container.scope/inner\n", mounts, &files),
            Some(4 << 30)
        );
    }

    #[test]
    fn cgroup_limit_is_found_in_the_usual_place_without_a_mount_of_the_group() {
        let groups = "12:memory:/docker/5f1e\n";
        let usual = "/sys/fs/cgroup/memory/docker/5f1e/memory.limit_in_bytes";
        let other = "/mnt/cgroup/memory.limit_in_bytes";
        let files = [(usual, "2147483648\n"), (other, "1\n")];
        // Mounts of other file systems, of hierarchies without the memory controller,
        // of the other version, and of a part of the hierarchy that the group is not in.
        for mounts in [
            "",
            "garbage\n",
            "24 30 0:22 / /proc rw,nosuid,nodev,noexec,relatime shared:13 - proc proc rw\n",
            "38 34 0:33 / /mnt/cgroup rw,relatime - cgroup cgroup rw,cpu,cpuacct\n",
            "38 34 0:33 / /mnt/cgroup rw,relatime - cgroup cgroup rw,name=memory\n",
            "35 25 0:30 / /mnt/cgroup rw,relatime - cgroup2 cgroup2 rw\n",
            "41 34 0:36 /docker/77aa /mnt/cgroup rw,relatime - cgroup cgroup rw,memory\n",
        ] {
            assert_eq!(
                limit_with_mounts(groups, mounts, &files),
                Some(2 << 30),
                "{mounts}"
            );
        }
    }
}
