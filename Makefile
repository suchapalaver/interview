test:
	@echo "Running test..."
	@cat test_input.txt | cargo run > output \
		&& diff output test_expected_output.txt \
		&& (echo "Test passed!" && rm output && exit 0) \
		|| (echo "Test failed!" && rm output && exit 1)
