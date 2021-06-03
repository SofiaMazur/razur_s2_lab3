package datastore

func NewWriteHandler(clb func () bool) *WriteHandler {
	return &WriteHandler{
		Req: make(chan entry),
		Res: make(chan error),
		closed: make(chan bool),
		onWriteClb: clb,
	}
}

type WriteHandler struct {
	Req chan entry
	Res chan error
	closed chan bool
	onWriteClb func() bool
}

func (wh *WriteHandler) StartLoop() {
	go func() {
		for {
			closed := wh.onWriteClb()
			if closed {
				break
			}
		}
		wh.closed <- true
	}()
}

func (wh *WriteHandler) Close() {
	close(wh.Req)
	close(wh.Res)
	<-wh.closed
}
