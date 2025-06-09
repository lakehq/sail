---
title: Troubleshooting
rank: 9
---

# Troubleshooting

## System Time Zone Issue on Amazon Linux

When you run Sail on Amazon Linux, you may encounter the following error when creating a Spark session:

```text
failed to get system time zone: No such file or directory (os error 2)
```

The reason is that `/etc/localtime` is supposed to be a symlink when retrieving the system time zone, but on Amazon Linux it is a regular file.
There is a [GitHub issue](https://github.com/amazonlinux/amazon-linux-2023/issues/526) for this problem, but it has not been resolved yet.

To work around this issue, you can run the following command on the host:

```bash
sudo timedatectl set-timezone UTC
```
