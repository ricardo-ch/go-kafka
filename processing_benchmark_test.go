package kafka

import "testing"

func BenchmarkProcessingPair(b *testing.B) {
	l := &listener{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l.beginProcessing()
		l.endProcessing()
	}
}

func BenchmarkProcessingPairParallel(b *testing.B) {
	l := &listener{}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.beginProcessing()
			l.endProcessing()
		}
	})
}
