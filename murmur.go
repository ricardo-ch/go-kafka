/*MIT License

Copyright (c) 2019 Alexandr Burdiyan

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

// Package kafka copied from https://github.com/burdiyan/kafkautil/blob/master/partitioner.go
// copied here to ensure this stay.
package kafka

import (
	"hash"

	"github.com/IBM/sarama"
)

// NewJVMCompatiblePartitioner creates a Sarama partitioner that uses
// the same hashing algorithm as JVM Kafka clients.
func NewJVMCompatiblePartitioner(topic string) sarama.Partitioner {
	return sarama.NewCustomHashPartitioner(MurmurHasher)(topic)
}

// murmurHash implements hash.Hash32 interface,
// solely to conform to required hasher for Sarama.
// it does not support streaming since it is not required for Sarama.
type murmurHash struct {
	v int32
}

// MurmurHasher creates murmur2 hasher implementing hash.Hash32 interface.
// The implementation is not full and does not support streaming.
// It only implements the interface to comply with sarama.NewCustomHashPartitioner signature.
// But Sarama only uses Write method once, when writing keys and values of the message,
// so streaming support is not necessary.
func MurmurHasher() hash.Hash32 {
	return new(murmurHash)
}

func (m *murmurHash) Write(d []byte) (n int, err error) {
	n = len(d)
	m.v = murmur2(d)
	return
}

func (m *murmurHash) Reset() {
	m.v = 0
}

func (m *murmurHash) Size() int { return 32 }

func (m *murmurHash) BlockSize() int { return 4 }

// Sum is noop.
func (m *murmurHash) Sum(in []byte) []byte {
	return in
}

func (m *murmurHash) Sum32() uint32 {
	return uint32(toPositive(m.v))
}

// murmur2 implements hashing algorithm used by JVM clients for Kafka.
// See the original implementation: https://github.com/apache/kafka/blob/1.0.0/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L353
func murmur2(data []byte) int32 {
	length := int32(len(data))
	seed := uint32(0x9747b28c)
	m := int32(0x5bd1e995)
	r := uint32(24)

	h := int32(seed ^ uint32(length))
	length4 := length / 4

	for i := int32(0); i < length4; i++ {
		i4 := i * 4
		k := int32(data[i4+0]&0xff) + (int32(data[i4+1]&0xff) << 8) + (int32(data[i4+2]&0xff) << 16) + (int32(data[i4+3]&0xff) << 24)
		k *= m
		k ^= int32(uint32(k) >> r)
		k *= m
		h *= m
		h ^= k
	}

	switch length % 4 {
	case 3:
		h ^= int32(data[(length & ^3)+2]&0xff) << 16
		fallthrough
	case 2:
		h ^= int32(data[(length & ^3)+1]&0xff) << 8
		fallthrough
	case 1:
		h ^= int32(data[length & ^3] & 0xff)
		h *= m
	}

	h ^= int32(uint32(h) >> 13)
	h *= m
	h ^= int32(uint32(h) >> 15)

	return h
}

// toPositive converts i to positive number as per the original implementation in the JVM clients for Kafka.
// See the original implementation: https://github.com/apache/kafka/blob/1.0.0/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L741
func toPositive(i int32) int32 {
	return i & 0x7fffffff
}
