package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func fatalOnError(err error) {
	if err != nil {
		tm := time.Now()
		fmt.Printf("Error(time=%+v):\nError: '%s'\nStacktrace:\n%s\n", tm, err.Error(), string(debug.Stack()))
		fmt.Fprintf(os.Stderr, "Error(time=%+v):\nError: '%s'\nStacktrace:\n", tm, err.Error())
		panic("stacktrace")
	}
}

func fatalf(f string, a ...interface{}) {
	fatalOnError(fmt.Errorf(f, a...))
}

func readOrgMapFile(fn string, uMap [3]map[string]map[string]struct{}) bool {
	f, err := os.Open(fn)
	fatalOnError(err)
	defer func() {
		_ = f.Close()
	}()
	space := regexp.MustCompile(`\s+`)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		txt := strings.TrimSpace(scanner.Text())
		if txt == "" || strings.HasPrefix(txt, "#") {
			continue
		}
		txt = strings.Replace(txt, ",", " ", -1)
		txt = space.ReplaceAllString(txt, " ")
		ary := strings.Split(txt, " ")
		name := ""
		emails := make(map[string]struct{})
		objIdx := 0
		for i, token := range ary {
			if strings.HasPrefix(token, "<") {
				if name == "" {
					fmt.Printf("i=%d token='%s' name='%s', emails=%+v\n", i, token, name, emails)
					fatalf("line: '%s'", txt)
				}
				email := strings.TrimSpace(token[1 : len(token)-1])
				emails[email] = struct{}{}
				continue
			}
			le := len(emails)
			if le > 0 {
				if le > 1 {
					fatalf("read more than 1 email: %+v for name: %s: '%s'\n", emails, name, txt)
				}
				fmt.Printf("%v: %s, finishing on token: %s\n", emails, name, token)
				objIdx++
				name = ""
				emails = make(map[string]struct{})
			}
			if name == "" {
				name = token
				continue
			}
			name += " " + token
		}
		le := len(emails)
		if le > 0 {
			if le > 1 {
				fatalf("read more than 1 email: %+v for name: %s: '%s'\n", emails, name, txt)
			}
			fmt.Printf("%v: %s, finished\n", emails, name)
			objIdx++
			name = ""
			emails = make(map[string]struct{})
		} else {
			fatalf("line '%s' ending on username, missing email(s)", txt)
		}
		if objIdx != 2 {
			fatalf("read more than 2 name-email(s) assignments: '%s'\n", txt)
		}
	}
	fatalOnError(scanner.Err())
	return true
}

func readMailMapFile(fn string) (ret [3]map[string]map[string]struct{}) {
	// names -> emails
	ret[0] = make(map[string]map[string]struct{})
	// emails -> names
	ret[1] = make(map[string]map[string]struct{})
	// correlations
	ret[2] = make(map[string]map[string]struct{})
	f, err := os.Open(fn)
	fatalOnError(err)
	defer func() {
		_ = f.Close()
	}()
	space := regexp.MustCompile(`\s+`)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		txt := strings.TrimSpace(scanner.Text())
		if txt == "" || strings.HasPrefix(txt, "#") {
			continue
		}
		txt = strings.Replace(txt, ",", " ", -1)
		txt = space.ReplaceAllString(txt, " ")
		// fmt.Printf("%s\n", txt)
		ary := strings.Split(txt, " ")
		name := ""
		emails := make(map[string]struct{})
		for i, token := range ary {
			if strings.HasPrefix(token, "<") {
				if name == "" {
					fmt.Printf("i=%d token='%s' name='%s', emails=%+v\n", i, token, name, emails)
					fatalf("line: '%s'", txt)
				}
				email := strings.TrimSpace(token[1 : len(token)-1])
				emails[email] = struct{}{}
				continue
			}
			if len(emails) > 0 {
				_, ok := ret[0][name]
				if !ok {
					ret[0][name] = make(map[string]struct{})
				}
				for email := range emails {
					_, ok := ret[1][email]
					if !ok {
						ret[1][email] = make(map[string]struct{})
					}
					ret[0][name][email] = struct{}{}
					ret[1][email][name] = struct{}{}
				}
				// fmt.Printf("%v: %s, finishing on token: %s\n", emails, name, token)
				name = ""
				emails = make(map[string]struct{})
			}
			if name == "" {
				name = token
				continue
			}
			name += " " + token
		}
		if len(emails) > 0 {
			_, ok := ret[0][name]
			if !ok {
				ret[0][name] = make(map[string]struct{})
			}
			for email := range emails {
				_, ok := ret[1][email]
				if !ok {
					ret[1][email] = make(map[string]struct{})
				}
				ret[0][name][email] = struct{}{}
				ret[1][email][name] = struct{}{}
			}
			// fmt.Printf("%v: %s, finished\n", emails, name)
			name = ""
			emails = make(map[string]struct{})
		} else {
			fmt.Printf("WARNING: line '%s' ending on username, missing email(s)\n", txt)
		}
	}
	fatalOnError(scanner.Err())
	for i := 0; i < 2; i++ {
		inf := []string{}
		for k, v := range ret[i] {
			l := len(v)
			if l > 1 {
				vs := []string{}
				for k2 := range v {
					vs = append(vs, "'"+k2+"'")
				}
				sort.Strings(vs)
				inf = append(inf, fmt.Sprintf("Key has %d values: '%s' -> %s", l, k, strings.Join(vs, " ")))
			}
		}
		sort.Strings(inf)
		fmt.Printf("%s\n", strings.Join(inf, "\n"))
	}
	// Check for correlations
	for i := 0; i < 2; i++ {
		j := 1 - i
		for k, m := range ret[i] {
			for v := range m {
				m2 := ret[j][v]
				for v2 := range m2 {
					if k != v2 {
						_, ok := ret[2][k]
						if !ok {
							ret[2][k] = make(map[string]struct{})
						}
						ret[2][k][v2] = struct{}{}
						_, ok = ret[2][v2]
						if !ok {
							ret[2][v2] = make(map[string]struct{})
						}
						ret[2][v2][k] = struct{}{}
						//fmt.Printf("%s <-> %s\n", k, v2)
					}
				}
			}
		}
	}
	return
}

func importMapfiles(db *sql.DB, mailMap, orgMap string) error {
	dbg := os.Getenv("DEBUG") != ""
	if dbg {
		fmt.Printf("Importing data from %s, %s files\n", mailMap, orgMap)
		rows, err := db.Query("select count(*) from profiles")
		fatalOnError(err)
		n := 0
		for rows.Next() {
			fatalOnError(rows.Scan(&n))
		}
		fatalOnError(rows.Err())
		fatalOnError(rows.Close())
		fmt.Printf("Number of profiles present in database: %d\n", n)
	}
	uData := readMailMapFile(mailMap)
	if dbg {
		fmt.Printf("Names => Emails:\n%+v\n", uData[0])
		fmt.Printf("Emails => Names:\n%+v\n", uData[1])
		fmt.Printf("Correlations:\n%+v\n", uData[2])
	}
	oData := readOrgMapFile(orgMap, uData)
	if dbg {
		fmt.Printf("Orgs:\n%+v\n", oData)
	}
	/* profiles
	+--------------+--------------+------+-----+---------+-------+
	| Field        | Type         | Null | Key | Default | Extra |
	+--------------+--------------+------+-----+---------+-------+
	| uuid         | varchar(128) | NO   | PRI | NULL    |       |
	| name         | varchar(128) | YES  |     | NULL    |       |
	| email        | varchar(128) | YES  |     | NULL    |       |
	| gender       | varchar(32)  | YES  |     | NULL    |       |
	| gender_acc   | int(11)      | YES  |     | NULL    |       |
	| is_bot       | tinyint(1)   | YES  |     | NULL    |       |
	| country_code | varchar(2)   | YES  | MUL | NULL    |       |
	+--------------+--------------+------+-----+---------+-------+
	*/
	/* identities
	+---------------+--------------+------+-----+---------+-------+
	| Field         | Type         | Null | Key | Default | Extra |
	+---------------+--------------+------+-----+---------+-------+
	| id            | varchar(128) | NO   | PRI | NULL    |       |
	| name          | varchar(128) | YES  | MUL | NULL    |       |
	| email         | varchar(128) | YES  |     | NULL    |       |
	| username      | varchar(128) | YES  |     | NULL    |       |
	| source        | varchar(32)  | NO   |     | NULL    |       |
	| uuid          | varchar(128) | YES  | MUL | NULL    |       |
	| last_modified | datetime(6)  | YES  |     | NULL    |       |
	+---------------+--------------+------+-----+---------+-------+
	*/
	/* organizations
	+-------+--------------+------+-----+---------+----------------+
	| Field | Type         | Null | Key | Default | Extra          |
	+-------+--------------+------+-----+---------+----------------+
	| id    | int(11)      | NO   | PRI | NULL    | auto_increment |
	| name  | varchar(191) | NO   | UNI | NULL    |                |
	+-------+--------------+------+-----+---------+----------------+
	*/
	/* enrollments
	+-----------------+--------------+------+-----+---------+----------------+
	| Field           | Type         | Null | Key | Default | Extra          |
	+-----------------+--------------+------+-----+---------+----------------+
	| id              | int(11)      | NO   | PRI | NULL    | auto_increment |
	| start           | datetime     | NO   |     | NULL    |                |
	| end             | datetime     | NO   |     | NULL    |                |
	| uuid            | varchar(128) | NO   | MUL | NULL    |                |
	| organization_id | int(11)      | NO   | MUL | NULL    |                |
	+-----------------+--------------+------+-----+---------+----------------+
	*/
	return nil
}

// getConnectString - get MariaDB SH (Sorting Hat) database DSN
// Either provide full DSN via SH_DSN='shuser:shpassword@tcp(shhost:shport)/shdb?charset=utf8&parseTime=true'
// Or use some SH_ variables, only SH_PASS is required
// Defaults are: "shuser:required_pwd@tcp(localhost:3306)/shdb?charset=utf8
// SH_DSN has higher priority; if set no SH_ varaibles are used
func getConnectString(prefix string) string {
	//dsn := "shuser:"+os.Getenv("PASS")+"@/shdb?charset=utf8")
	dsn := os.Getenv(prefix + "DSN")
	if dsn == "" {
		pass := os.Getenv(prefix + "PASS")
		user := os.Getenv(prefix + "USR")
		if user == "" {
			user = os.Getenv(prefix + "USER")
		}
		proto := os.Getenv(prefix + "PROTO")
		if proto == "" {
			proto = "tcp"
		}
		host := os.Getenv(prefix + "HOST")
		if host == "" {
			host = "localhost"
		}
		port := os.Getenv(prefix + "PORT")
		if port == "" {
			port = "3306"
		}
		db := os.Getenv(prefix + "DB")
		if db == "" {
			fatalf("please specify database via %sDB=...", prefix)
		}
		params := os.Getenv(prefix + "PARAMS")
		if params == "" {
			params = "?charset=utf8&parseTime=true"
		}
		if params == "-" {
			params = ""
		}
		dsn = fmt.Sprintf(
			"%s:%s@%s(%s:%s)/%s%s",
			user,
			pass,
			proto,
			host,
			port,
			db,
			params,
		)
	}
	return dsn
}

func main() {
	// Connect to MariaDB
	if len(os.Args) < 3 {
		fmt.Printf("Arguments required: mail_mapfile org_mapfile\n")
		return
	}
	var db *sql.DB
	dsn := getConnectString("SH_")
	db, err := sql.Open("mysql", dsn)
	fatalOnError(err)
	defer func() { fatalOnError(db.Close()) }()
	fatalOnError(importMapfiles(db, os.Args[1], os.Args[2]))
}
