package shared

type JoinArgs struct {
	Username     string
	CallbackAddr string
}

type SendArgs struct {
	From string
	Text string
}

type LeaveArgs struct {
	Username string
}

type UsersArgs struct{}

type UsersReply struct {
	Users []string
}

type Event struct {
	Kind string
	From string
	Text string
}
