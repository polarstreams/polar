package ownership

import (
	"testing"

	. "github.com/jorgebay/soda/internal/test/discovery/mocks"
	. "github.com/jorgebay/soda/internal/test/interbroker/mocks"
	. "github.com/jorgebay/soda/internal/test/localdb/mocks"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Ownership Suite")
}

var _ = Describe("generator", func() {
	Describe("startNew()", func() {
		It("Should create a new local generation", func() {
			dbMock := new(Client)
			dbMock.On("DbWasNewlyCreated").Return(true)

			gossiperMock := new(Gossiper)
			discoverer := new(Discoverer)
			discoverer.On("LocalInfo").Return()

			o := &generator{
				discoverer: discoverer,
				gossiper:   gossiperMock,
				localDb:    dbMock,
				items:      make(chan genMessage),
			}

			go o.process()

			Expect(dbMock.DbWasNewlyCreated()).To(Equal(true))
		})
	})
})
