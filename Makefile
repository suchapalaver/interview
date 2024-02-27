test:
	@echo "Running test..."
	@cat test_input.txt | cargo run > output1 \
		&& diff output1 test_expected_output.txt \
		&& (echo "Test passed!" && rm output1 && exit 0) \
		|| (echo "Test failed!" && rm output1 && exit 1)

test-input:
	@echo "Running test..."
	@cat input.txt | cargo run > output2 \
		&& diff output2 expected_output.txt \
		&& (echo "Test passed!" && rm output2 && exit 0) \
		|| (echo "Test failed!" && rm output2 && exit 1)
