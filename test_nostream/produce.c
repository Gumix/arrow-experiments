#include <stddef.h>
#include <stdlib.h>
#include <assert.h>
#include "../arrow.h"

const int64_t column_count = 3;
const int64_t batch_count = 5;
const int64_t row_in_batch_count = 10;
const int64_t row_count = row_in_batch_count * batch_count;

const char *column_names[] = { "col1", "col2", "col3" };

struct column {
	struct ArrowSchema schema;
	struct ArrowArray array;
};

static void
release_static_type(struct ArrowSchema *schema)
{
	/* Mark released. */
	schema->release = NULL;
}

static void
release_malloced_array(struct ArrowArray *array)
{
	assert(array->n_buffers == 2);
	/* Free the buffers and the buffers array. */
	for (int i = 0; i < array->n_buffers; i++)
		free((void *) array->buffers[i]);
	free(array->buffers);
	/* Mark released. */
	array->release = NULL;
}

static void
gen_schema(struct ArrowSchema *schema)
{
	*schema = (struct ArrowSchema) {
		/* Type description (uint32). */
		.format = "I",
		.name = "",
		.metadata = NULL,
		.flags = 0,
		.n_children = 0,
		.children = NULL,
		.dictionary = NULL,
		/* Bookkeeping. */
		.release = &release_static_type
	};
}

static int64_t row_processed;

static void
gen_array(struct ArrowArray *array, int c)
{
	/*
	 * Initialize primitive fields.
	 */
	*array = (struct ArrowArray) {
		/* Data description. */
		.length = row_in_batch_count,
		.offset = 0,
		.null_count = 0,
		.n_buffers = 2,
		.n_children = 0,
		.children = NULL,
		.dictionary = NULL,
		/* Bookkeeping. */
		.release = &release_malloced_array
	};
	/* Allocate list of buffers. */
	size_t size = sizeof(void*) * array->n_buffers;
	array->buffers = (const void**) malloc(size);
	assert(array->buffers != NULL);

	/*
	 * TODO: nulls_size = (row_in_batch_count / CHAR_BIT) + padding
	 * to a length that is a multiple of 64 bytes.
	 */
	size_t nulls_size = 64;
	array->buffers[0] = aligned_alloc(64, nulls_size);
	uint8_t *null = (uint8_t *) array->buffers[0];
	for (int r = 0; r < nulls_size; r++) {
		if (c == 1)
			null[r] = 0xAA;
		else if (c == 2)
			null[r] = 0x55;
		else
			null[r] = 0x00;
	}

	size_t data_size = sizeof(uint32_t) * row_in_batch_count;
	array->buffers[1] = aligned_alloc(64, data_size);
	uint32_t *col = (uint32_t *) array->buffers[1];
	for (int r = 0; r < array->length; r++) {
		uint32_t val = (c + 1) * 10000 + row_processed + r;
		col[r] = val;
	}
}

void
gen_columns(struct column *columns)
{
	if (row_processed >= row_count) {
		columns[0].array.length = 0;
		return;
	}

	for (int c = 0; c < column_count; c++) {
		struct column *col = &columns[c];
		gen_schema(&col->schema);
		gen_array(&col->array, c);
	}

	row_processed += row_in_batch_count;
}
