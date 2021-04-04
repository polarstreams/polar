package producing

const maxLength = 32 * 1024

type coalescer struct {
	items   chan int //TODO: type item
	limiter chan bool
}

func (c *coalescer) receive() {
	for {
		group := make([]int, 0) // item
		length := 0
		item := <-c.items

		group = append(group, item)
		length += item

		for length < maxLength {
			// receive without blocking
			select {
			case item := <-c.items:
				group = append(group, item)
				length += item
			default:
				//
				break
			}
		}

		c.limiter <- true

		go c.compressAndSend()
		// compress and crc
		// send in the background
	}
}

func (c *coalescer) compressAndSend() {
	// TODO: implement

	// Allow the next to be sent
	<-c.limiter
}
