package record

type Record struct {
	Name string `json:"user_name"`
	User string `json:"user"`
	ID   int
	Age  int `json:"-"`
}
