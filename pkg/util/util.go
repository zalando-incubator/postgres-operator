package util

import (
	"crypto/hmac"
	"crypto/md5" // #nosec we need it to for PostgreSQL md5 passwords
	cryptoRand "crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/big"
	"math/rand"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/motomux/pretty"
	resource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/zalando/postgres-operator/pkg/spec"
	"golang.org/x/crypto/pbkdf2"
)

const (
	md5prefix         = "md5"
	scramsha256prefix = "SCRAM-SHA-256"
	saltlength        = 16
	iterations        = 4096
)

var passwordChars = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func init() {
	rand.Seed(time.Now().Unix())
}

// helper function to get bool pointers
func True() *bool {
	b := true
	return &b
}

func False() *bool {
	b := false
	return &b
}

// RandomPassword generates a secure, random alphanumeric password of a given length.
func RandomPassword(n int) string {
	b := make([]byte, n)
	for i := range b {
		maxN := big.NewInt(int64(len(passwordChars)))
		if n, err := cryptoRand.Int(cryptoRand.Reader, maxN); err != nil {
			panic(fmt.Errorf("Unable to generate secure, random password: %v", err))
		} else {
			b[i] = passwordChars[n.Int64()]
		}
	}
	return string(b)
}

// NameFromMeta converts a metadata object to the NamespacedName name representation.
func NameFromMeta(meta metav1.ObjectMeta) spec.NamespacedName {
	return spec.NamespacedName{
		Namespace: meta.Namespace,
		Name:      meta.Name,
	}
}

type Hasher func(user spec.PgUser) string
type Random func(n int) string

type Encryptor struct {
	encrypt Hasher
	random  Random
}

func NewEncryptor(encryption string) *Encryptor {
	e := Encryptor{random: RandomPassword}
	m := map[string]Hasher{
		"md5":           e.PGUserPasswordMD5,
		"scram-sha-256": e.PGUserPasswordScramSHA256,
	}
	hasher, ok := m[encryption]
	if !ok {
		hasher = e.PGUserPasswordMD5
	}
	e.encrypt = hasher
	return &e
}

func (e *Encryptor) PGUserPassword(user spec.PgUser) string {
	if (len(user.Password) == md5.Size*2+len(md5prefix) && user.Password[:3] == md5prefix) ||
		(len(user.Password) > len(scramsha256prefix) && user.Password[:len(scramsha256prefix)] == scramsha256prefix) || user.Password == "" {
		// Avoid processing already encrypted or empty passwords
		return user.Password
	}
	return e.encrypt(user)
}

func (e *Encryptor) PGUserPasswordMD5(user spec.PgUser) string {
	s := md5.Sum([]byte(user.Password + user.Name)) // #nosec, using md5 since PostgreSQL uses it for hashing passwords.
	return md5prefix + hex.EncodeToString(s[:])
}

func (e *Encryptor) PGUserPasswordScramSHA256(user spec.PgUser) string {
	salt := []byte(e.random(saltlength))
	key := pbkdf2.Key([]byte(user.Password), salt, iterations, 32, sha256.New)
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("Server Key"))
	serverKey := mac.Sum(nil)
	mac = hmac.New(sha256.New, key)
	mac.Write([]byte("Client Key"))
	clientKey := mac.Sum(nil)
	storedKey := sha256.Sum256(clientKey)
	pass := fmt.Sprintf("%s$%v:%s$%s:%s",
		scramsha256prefix,
		iterations,
		base64.StdEncoding.EncodeToString(salt),
		base64.StdEncoding.EncodeToString(storedKey[:]),
		base64.StdEncoding.EncodeToString(serverKey),
	)
	return pass
}

// Diff returns diffs between 2 objects
func Diff(a, b interface{}) []string {
	return pretty.Diff(a, b)
}

// PrettyDiff shows the diff between 2 objects in an easy to understand format. It is mainly used for debugging output.
func PrettyDiff(a, b interface{}) string {
	return strings.Join(Diff(a, b), "\n")
}

// Compare two string slices while ignoring the order of elements
func IsEqualIgnoreOrder(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	a_copy := make([]string, len(a))
	b_copy := make([]string, len(b))
	copy(a_copy, a)
	copy(b_copy, b)
	sort.Strings(a_copy)
	sort.Strings(b_copy)

	return reflect.DeepEqual(a_copy, b_copy)
}

// SliceReplaceElement
func StringSliceReplaceElement(s []string, a, b string) (result []string) {
	tmp := make([]string, 0, len(s))
	for _, str := range s {
		if str == a {
			str = b
		}
		tmp = append(tmp, str)
	}
	return tmp
}

// SubstractStringSlices finds elements in a that are not in b and return them as a result slice.
func SubstractStringSlices(a []string, b []string) (result []string, equal bool) {
	// Slices are assumed to contain unique elements only
OUTER:
	for _, vala := range a {
		for _, valb := range b {
			if vala == valb {
				continue OUTER
			}
		}
		result = append(result, vala)
	}
	return result, len(result) == 0
}

// FindNamedStringSubmatch returns a map of strings holding the text of the matches of the r regular expression
func FindNamedStringSubmatch(r *regexp.Regexp, s string) map[string]string {
	matches := r.FindStringSubmatch(s)
	grNames := r.SubexpNames()

	if matches == nil {
		return nil
	}

	groupMatches := 0
	res := make(map[string]string, len(grNames))
	for i, n := range grNames {
		if n == "" {
			continue
		}

		res[n] = matches[i]
		groupMatches++
	}

	if groupMatches == 0 {
		return nil
	}

	return res
}

// SliceContains
func SliceContains(slice interface{}, item interface{}) bool {
	s := reflect.ValueOf(slice)
	if s.Kind() != reflect.Slice {
		panic("Invalid data-type")
	}
	for i := 0; i < s.Len(); i++ {
		if s.Index(i).Interface() == item {
			return true
		}
	}
	return false
}

// MapContains returns true if and only if haystack contains all the keys from the needle with matching corresponding values
func MapContains(haystack, needle map[string]string) bool {
	if len(haystack) < len(needle) {
		return false
	}

	for k, v := range needle {
		v2, ok := haystack[k]
		if !ok || v2 != v {
			return false
		}
	}

	return true
}

// Coalesce returns the first argument if it is not null, otherwise the second one.
func Coalesce(val, defaultVal string) string {
	if val == "" {
		return defaultVal
	}
	return val
}

// CoalesceStrArr returns the first argument if it is not null, otherwise the second one.
func CoalesceStrArr(val, defaultVal []string) []string {
	if len(val) == 0 {
		return defaultVal
	}
	return val
}

// CoalesceStrMap returns the first argument if it is not null, otherwise the second one.
func CoalesceStrMap(val, defaultVal map[string]string) map[string]string {
	if len(val) == 0 {
		return defaultVal
	}
	return val
}

// CoalesceInt works like coalesce but for int
func CoalesceInt(val, defaultVal int) int {
	if val == 0 {
		return defaultVal
	}
	return val
}

// CoalesceInt32 works like coalesce but for *int32
func CoalesceInt32(val, defaultVal *int32) *int32 {
	if val == nil {
		return defaultVal
	}
	return val
}

// CoalesceUInt32 works like coalesce but for uint32
func CoalesceUInt32(val, defaultVal uint32) uint32 {
	if val == 0 {
		return defaultVal
	}
	return val
}

// CoalesceInt64 works like coalesce but for int64
func CoalesceInt64(val, defaultVal int64) int64 {
	if val == 0 {
		return defaultVal
	}
	return val
}

// CoalesceBool works like coalesce but for *bool
func CoalesceBool(val, defaultVal *bool) *bool {
	if val == nil {
		return defaultVal
	}
	return val
}

// CoalesceDuration works like coalesce but for time.Duration
func CoalesceDuration(val time.Duration, defaultVal string) time.Duration {
	if val == 0 {
		duration, err := time.ParseDuration(defaultVal)
		if err != nil {
			panic(err)
		}
		return duration
	}
	return val
}

// Test if any of the values is nil
func testNil(values ...*int32) bool {
	for _, v := range values {
		if v == nil {
			return true
		}
	}

	return false
}

// MaxInt32 : Return maximum of two integers provided via pointers. If one value
// is not defined, return the other one. If both are not defined, result is also
// undefined, caller needs to check for that.
func MaxInt32(a, b *int32) *int32 {
	if testNil(a, b) {
		return nil
	}

	if *a > *b {
		return a
	}

	return b
}

// IsSmallerQuantity : checks if first resource is of a smaller quantity than the second
func IsSmallerQuantity(requestStr, limitStr string) (bool, error) {

	request, err := resource.ParseQuantity(requestStr)
	if err != nil {
		return false, fmt.Errorf("could not parse request %v : %v", requestStr, err)
	}

	limit, err2 := resource.ParseQuantity(limitStr)
	if err2 != nil {
		return false, fmt.Errorf("could not parse limit %v : %v", limitStr, err2)
	}

	if limitStr == "0" { // Zero disables setting a limit
		return false, nil
	}

	return request.Cmp(limit) == -1, nil
}
