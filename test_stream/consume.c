#include <dlfcn.h>
#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdbool.h>
#include "../arrow.h"

typedef void (*init_stream_fn)(struct ArrowArrayStream *stream);

static void
dump_chunk(struct ArrowArray *array, struct ArrowSchema *schema, int chunk_num)
{
	int64_t num_cols = array->n_children;
	int64_t num_rows = array->length;

	printf("\nchunk %d:\n\t", chunk_num);
	for (int c = 0; c < num_cols; c++)
		printf("[%s]\t", schema->children[c]->name);
	printf("\n");

	for (int r = 0; r < num_rows ; r++) {
		printf("[%d]\t", r);
		for (int c = 0; c < num_cols; c++) {
			struct ArrowArray *col = array->children[c];
			uint8_t *nulls = (uint8_t *) col->buffers[0];
			uint32_t *vals = (uint32_t *) col->buffers[1];

			bool is_valid = nulls[r / 8] & (1u << (r % 8));
			if (is_valid)
				printf("%u\t", vals[r]);
			else
				printf("null\t");
		}
		printf("\n");
	}
}

int main()
{
	void *h = dlopen("produce.so", RTLD_LAZY);
	if (h == NULL) {
		fprintf(stderr, "%s\n", dlerror());
		exit(EXIT_FAILURE);
	}
	/* Clear any existing error. */
	dlerror();

	init_stream_fn init_stream = dlsym(h, "init_stream");
	if (init_stream == NULL) {
		fprintf(stderr, "%s\n", dlerror());
		exit(EXIT_FAILURE);
	}
	/*
	 * Initialize the stream.
	 */
	struct ArrowArrayStream stream;
	init_stream(&stream);
	/*
	 * Query result set schema.
	 */
	struct ArrowSchema schema;
	int errcode = stream.get_schema(&stream, &schema);
	if (errcode != 0) {
		fprintf(stderr, "stream.get_schema() failed\n");
		exit(EXIT_FAILURE);
	}
	/*
	 * Iterate over results.
	 */
	for (int chunk_num = 0; true; chunk_num++) {
		struct ArrowArray array;
		errcode = stream.get_next(&stream, &array);
		if (errcode != 0) {
			fprintf(stderr, "stream.get_next() failed\n");
			exit(EXIT_FAILURE);
		}

		/* Loop until end of stream. */	
		if (array.length == 0)
			break;

		/* Process array chunk. */
		dump_chunk(&array, &schema, chunk_num);

		/* Release array chunk. */
		array.release(&array);
	}

	/* Release schema and stream. */
	schema.release(&schema);
	stream.release(&stream);

	dlclose(h);
	return 0;
}
