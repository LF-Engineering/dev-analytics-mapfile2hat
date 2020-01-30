# mapfile2hat
Import .mailmap and .organizationmap files into SortingHat

# Usage

- Start local MariaDB server via: `PASS=rootpwd ./mariadb_local_docker.sh`.
- Connect to local MariaDB server via: `USR=root PASS=rootpwd ./mariadb_shell.sh` to test database connection.
- Initialize SortingHat user & database: `USR=root PASS=rootpwd SH_USR=shusername SH_PASS=shpwd SH_DB=shdb ./mariadb_init.sh`.
- To drop SortingHat database & user (just an util script): `USR=root PASS=rootpwd SH_USR=shusername SH_DB=shdb ./mariadb_init.sh`.
