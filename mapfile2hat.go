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

// unaffiliated - special company name for unaffiliated users
const unaffiliated string = "Unaffiliated"

type affData struct {
	Names  []string
	Emails []string
	Org    [2]string
}

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

func readOrgMapFile(dbg bool, fn string, uMap [3]map[string]map[string]struct{}) (result []affData) {
	var (
		comps [2]map[string]string
		users [2]map[string]string
		affs  [2]map[[2]string]map[[2]string]struct{}
	)
	// comps name -> email
	comps[0] = make(map[string]string)
	// comps email -> name
	comps[1] = make(map[string]string)
	// users name -> email
	users[0] = make(map[string]string)
	// users email -> name
	users[1] = make(map[string]string)
	// affs company -> users
	affs[0] = make(map[[2]string]map[[2]string]struct{})
	// affs user -> companies
	affs[1] = make(map[[2]string]map[[2]string]struct{})
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
		compEmail := ""
		userEmail := ""
		compName := ""
		userName := ""
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
				// fmt.Printf("%v: %s, finishing on token: %s\n", emails, name, token)
				email := ""
				for em := range emails {
					email = em
					break
				}
				if objIdx == 0 {
					comps[0][name] = email
					comps[1][email] = name
					compEmail = email
					compName = name
				} else {
					users[0][name] = email
					users[1][email] = name
					userEmail = email
					userName = name
				}
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
			// fmt.Printf("%v: %s, finished\n", emails, name)
			email := ""
			for em := range emails {
				email = em
				break
			}
			if objIdx == 0 {
				comps[0][name] = email
				comps[1][email] = name
				compEmail = email
				compName = name
			} else {
				users[0][name] = email
				users[1][email] = name
				userEmail = email
				userName = name
			}
			objIdx++
			name = ""
			emails = make(map[string]struct{})
		} else {
			fatalf("line '%s' ending on username, missing email(s)", txt)
		}
		if objIdx != 2 {
			fatalf("read more than 2 name-email(s) assignments: '%s'\n", txt)
		}
		u := [2]string{userName, userEmail}
		c := [2]string{compName, compEmail}
		_, ok := affs[0][c]
		if !ok {
			affs[0][c] = make(map[[2]string]struct{})
		}
		affs[0][c][u] = struct{}{}
		_, ok = affs[1][u]
		if !ok {
			affs[1][u] = make(map[[2]string]struct{})
		}
		affs[1][u][c] = struct{}{}
	}
	fatalOnError(scanner.Err())
	inf := []string{}
	/*
		fmt.Printf("comp -> users\n")
		for k, v := range affs[0] {
			fmt.Printf("%v: %v\n", k, v)
		}
		fmt.Printf("user -> comps\n")
		for k, v := range affs[1] {
			fmt.Printf("%v: %v\n", k, v)
		}
	*/
	for i := 0; i < 2; i++ {
		for k, v := range affs[i] {
			l := len(v)
			if l > 1 {
				vs := []string{}
				for k2 := range v {
					vs = append(vs, "'"+k2[0]+","+k2[1]+"'")
				}
				sort.Strings(vs)
				msg := fmt.Sprintf("Key has %d values: '%s' -> %s", l, k, strings.Join(vs, " "))
				if i == 1 {
					fatalf(msg)
				}
				inf = append(inf, msg)
			}
		}
	}
	sort.Strings(inf)
	if dbg {
		fmt.Printf("%s\n", strings.Join(inf, "\n"))
	}
	for usr, data := range affs[1] {
		userName := usr[0]
		userEmail := usr[1]
		names, okNames := uMap[1][userEmail]
		emails, okEmails := uMap[0][userName]
		uEmails := make(map[string]struct{})
		uNames := make(map[string]struct{})
		uNames[userName] = struct{}{}
		if okNames {
			for name := range names {
				uNames[name] = struct{}{}
			}
		}
		uEmails[userEmail] = struct{}{}
		if okEmails {
			for email := range emails {
				uEmails[email] = struct{}{}
			}
		}
		nameCorrels, okNameCorels := uMap[2][userName]
		emailCorrels, okEmailCorels := uMap[2][userEmail]
		if okNameCorels {
			//fmt.Printf("N Found by correlations (%s,%s) -> ((%v,%v),(%v,%v)) -> ((%v,%v),(%v,%v))\n", userName, userEmail, names, okNames, emails, okEmails, nameCorrels, okNameCorels, emailCorrels, okEmailCorels)
			for name := range nameCorrels {
				uNames[name] = struct{}{}
			}
		}
		if okEmailCorels {
			//fmt.Printf("E Found by correlations (%s,%s) -> ((%v,%v),(%v,%v)) -> ((%v,%v),(%v,%v))\n", userName, userEmail, names, okNames, emails, okEmails, nameCorrels, okNameCorels, emailCorrels, okEmailCorels)
			for email := range emailCorrels {
				uEmails[email] = struct{}{}
			}
		}
		aEmails := []string{}
		for email := range uEmails {
			aEmails = append(aEmails, email)
		}
		aNames := []string{}
		for name := range uNames {
			aNames = append(aNames, name)
		}
		company := [2]string{}
		for c := range data {
			company = c
			break
		}
		if dbg {
			fmt.Printf("(%d:%v,%d:%v) -> %v\n", len(aNames), aNames, len(aEmails), aEmails, company)
		}
		result = append(result, affData{Names: aNames, Emails: aEmails, Org: company})
	}
	return
}

func readMailMapFile(dbg bool, fn string) (ret [3]map[string]map[string]struct{}) {
	// name -> emails
	ret[0] = make(map[string]map[string]struct{})
	// email -> names
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
		if dbg {
			fmt.Printf("%s\n", strings.Join(inf, "\n"))
		}
	}
	// Check for correlations
	for i := 0; i < 2; i++ {
		j := 1 - i
		for k, m := range ret[i] {
			for v := range m {
				m2 := ret[j][v]
				for v2 := range m2 {
					if k != v2 {
						// to have correlate only by email, name is not that unique user "if i == 1 {"
						// current condition means that we want all correlations
						if i < 2 {
							if dbg {
								fmt.Printf("Correlation %s <-- %s --> %s\n", k, v, v2)
							}
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
						}
					}
				}
			}
		}
	}
	return
}

func addOrganization(db *sql.DB, company string) int {
	_, err := db.Exec("insert into organizations(name) values(?)", company)
	if err != nil {
		if strings.Contains(err.Error(), "Error 1062") {
			rows, err2 := db.Query("select name from organizations where name = ?", company)
			fatalOnError(err2)
			var existingName string
			for rows.Next() {
				fatalOnError(rows.Scan(&existingName))
			}
			fatalOnError(rows.Err())
			fatalOnError(rows.Close())
			// fmt.Printf("Warning: name collision: trying to insert '%s', exists: '%s'\n", company, existingName)
		} else {
			fatalOnError(err)
		}
	}
	rows, err := db.Query("select id from organizations where name = ?", company)
	fatalOnError(err)
	var id int
	fetched := false
	for rows.Next() {
		fatalOnError(rows.Scan(&id))
		fetched = true
	}
	fatalOnError(rows.Err())
	fatalOnError(rows.Close())
	if !fetched {
		fatalf("failed to add '%s' company", company)
	}
	return id
}

func findIdentities(db *sql.DB, names, emails []string) (uuids []string) {
	m := make(map[string]struct{})
	if len(names) > 0 {
		query := "select uuid from identities where name in ("
		args := []interface{}{}
		for _, name := range names {
			query += "?,"
			args = append(args, name)
		}
		query = query[:len(query)-1] + ")"
		rows, err := db.Query(query, args...)
		fatalOnError(err)
		uuid := ""
		for rows.Next() {
			fatalOnError(rows.Scan(&uuid))
			m[uuid] = struct{}{}
		}
		fatalOnError(rows.Err())
		fatalOnError(rows.Close())
	}
	if len(emails) > 0 {
		query := "select uuid from identities where email in ("
		args := []interface{}{}
		for _, email := range emails {
			query += "?,"
			args = append(args, email)
		}
		query = query[:len(query)-1] + ")"
		rows, err := db.Query(query, args...)
		fatalOnError(err)
		uuid := ""
		for rows.Next() {
			fatalOnError(rows.Scan(&uuid))
			m[uuid] = struct{}{}
		}
		fatalOnError(rows.Err())
		fatalOnError(rows.Close())
	}
	for uuid := range m {
		uuids = append(uuids, uuid)
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
	uData := readMailMapFile(dbg, mailMap)
	if dbg {
		fmt.Printf("Names => Emails:\n%+v\n", uData[0])
		fmt.Printf("Emails => Names:\n%+v\n", uData[1])
		fmt.Printf("Correlations:\n%+v\n", uData[2])
	}
	affs := readOrgMapFile(dbg, orgMap, uData)
	if dbg {
		fmt.Printf("Affs:\n%+v\n", affs)
	}
	comp2id := make(map[string]int)
	for _, aff := range affs {
		comp := aff.Org[0]
		if comp == unaffiliated {
			continue
		}
		comp2id[aff.Org[0]] = 0
	}
	for comp := range comp2id {
		comp2id[comp] = addOrganization(db, comp)
		if dbg {
			fmt.Printf("Org '%s' -> %d\n", comp, comp2id[comp])
		}
	}
	for _, aff := range affs {
		comp := aff.Org[0]
		if comp == unaffiliated {
			continue
		}
		names := aff.Names
		emails := aff.Emails
		uuids := findIdentities(db, names, emails)
		if len(uuids) == 0 {
			fmt.Printf("No identities found for names=%v emails=%v\n", names, emails)
			continue
		}
		compID := comp2id[comp]
		if dbg {
			fmt.Printf("Enroll %v to %s/%d\n", uuids, comp, compID)
		}
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
	dtStart := time.Now()
	var db *sql.DB
	dsn := getConnectString("SH_")
	db, err := sql.Open("mysql", dsn)
	fatalOnError(err)
	defer func() { fatalOnError(db.Close()) }()
	fatalOnError(importMapfiles(db, os.Args[1], os.Args[2]))
	dtEnd := time.Now()
	fmt.Printf("Time(%s): %v\n", os.Args[0], dtEnd.Sub(dtStart))
}
