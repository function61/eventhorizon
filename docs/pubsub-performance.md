Performance

	Initial:

		450 000 msgs/sec

	Write channel buffer increase 10 -> 100:

		519 000 msgs/sec

	Server write batching, batchSize=4k

		744 000 msgs/sec

	Server write batching, batchSize=8k

		723 000 msgs/sec

	Server write batching, batchSize=2k

		763 000 msgs/sec

	Writes go now through channels

		503 326 msgs/sec


	Observations:

	- No improvement when getting rid of msgformatEncode()

	Methodology:

	- Within a single container: Producer -> Server -> Consumer
