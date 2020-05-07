# mapfile2hat
Import .mailmap and .organizationmap files into SortingHat

# Usage

- Start local MariaDB server via: `PASS=rootpwd ./mariadb_local_docker.sh`.
- Connect to local MariaDB server via: `USR=root PASS=rootpwd ./mariadb_root_shell.sh` to test database connection.
- Initialize SortingHat user & database: `USR=root PASS=rootpwd SH_USR=shusername SH_PASS=shpwd SH_DB=shdb [FULL=1] ./mariadb_init.sh`.
- If `FULL=1` is specified, SortingHat database will be created from gitignored populated `sortinghat.sql` file instead of an empty structure file `structure.sql`.
- To drop SortingHat database & user (just an util script): `USR=root PASS=rootpwd SH_USR=shusername SH_DB=shdb ./mariadb_init.sh`.
- Connect to SortingHat database via: `SH_USR=shusername SH_PASS=shpwd SH_DB=shdb ./mariadb_sortinghat_shell.sh` to test SortingHat database connection.
- To import data form `.mailmap` and `.organizationmap` files do: `[REPLACE=1] SH_USR=shusername SH_PASS=shpwd SH_DB=shdb SH_PORT=13306 [DEBUG=1] ./mapfile2hat .mailmap .organizationmap`
- If you specify `REPLACE=1` it will delete existing enrollments and insert new ones on conflict.  Otherwise it will not add enrollments if there is a conflict.
- Typical usage inside MariaDB K8s pods with all env defined by pod: `REPLACE=1 ./mapfile2hat .mailmap .organizationmap`.
- If using manual `SH_DSN` - remember to add option `parseTime=true`.
