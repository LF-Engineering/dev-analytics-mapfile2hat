# mapfile2hat
Import .mailmap and .organizationmap files into SortingHat

# Usage

- Start local MariaDB server via: `PASS=rootpwd ./mariadb_local_docker.sh`.
- Connect to local MariaDB server via: `USR=root PASS=rootpwd ./mariadb_root_shell.sh` to test database connection.
- Initialize SortingHat user & database: `USR=root PASS=rootpwd SH_USR=shusername SH_PASS=shpwd SH_DB=shdb [FULL=1] ./mariadb_init.sh`.
- If `FULL=1` is specified, SortingHat database will be created from gitignored populated `sortinghat.sql` file instead of an empty structure file `structure.sql`.
- To drop SortingHat database & user (just an util script): `USR=root PASS=rootpwd SH_USR=shusername SH_DB=shdb ./mariadb_init.sh`.
- Connect to SortingHat database via: `SH_USR=shusername SH_PASS=shpwd SH_DB=shdb ./mariadb_sortinghat_shell.sh` to test SortingHat database connection.
