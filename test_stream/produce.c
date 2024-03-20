#include <stddef.h>
#include <stdlib.h>
#include "../arrow.h"

const int64_t column_count = 3;
const int64_t batch_count = 5;
const int64_t row_in_batch_count = 10;
const int64_t row_count = row_in_batch_count * batch_count;

const char *column_names[] = { "col1", "col2", "col3" };

static void
release_stream(struct ArrowArrayStream *stream)
{
	/* Nop. */
}

static void
release_malloced_type(struct ArrowSchema *schema)
{
	for (int i = 0; i < schema->n_children; i++) {
		struct ArrowSchema *child = schema->children[i];
		if (child->release != NULL)
			child->release(child);
		free(child);
	}
	free(schema->children);
	/* Mark released. */
	schema->release = NULL;
}

static void
release_malloced_array(struct ArrowArray *array)
{
	/* Free children. */
	for (int i = 0; i < array->n_children; i++) {
		struct ArrowArray* child = array->children[i];
		if (child->release != NULL)
			child->release(child);
		free(child);
	}
	free(array->children);
	/* Free buffers. */
	for (int i = 0; i < array->n_buffers; i++)
		free((void *) array->buffers[i]);
	free(array->buffers);
	/* Mark released. */
	array->release = NULL;
}

/**
 * Record batches.
 *
 * A record batch can be trivially considered as an equivalent struct array.
 * In this case the metadata of the top-level ArrowSchema can be used for the
 * schema-level metadata of the record batch.
 */
static int
gen_schema(struct ArrowArrayStream *stream, struct ArrowSchema *schema)
{
	/*
	 * Initialize parent type.
	 */
	*schema = (struct ArrowSchema) {
		/* Type description (struct). */
		.format = "+s",
		.name = "",
		.metadata = NULL,
		.flags = 0,
		.n_children = column_count,
		.dictionary = NULL,
		/* Bookkeeping. */
		.release = &release_malloced_type
	};
	/*
	 * Allocate list of children types.
	 */
	size_t size = sizeof(struct ArrowSchema *) * schema->n_children;
	schema->children = malloc(size);
	/*
	 * Initialize children types.
	 */
	for (int i = 0; i < schema->n_children; i++) {
		size_t size = sizeof(struct ArrowSchema);
		struct ArrowSchema *child = schema->children[i] = malloc(size);
		*child = (struct ArrowSchema) {
			/* Type description (uint32). */
			.format = "I",
			.name = column_names[i],
			.metadata = NULL,
			.flags = ARROW_FLAG_NULLABLE,
			.n_children = 0,
			.dictionary = NULL,
			.children = NULL,
			/* Bookkeeping. */
			.release = &release_malloced_type
		};
	}
	return 0;
}

static int
gen_array_chunk(struct ArrowArrayStream *stream, struct ArrowArray *array)
{
	static int64_t row_processed;
	if (row_processed >= row_count) {
		array->length = 0;
		return 0;
	}
	/*
	 * Initialize parent array.
	 */
	*array = (struct ArrowArray) {
		/* Data description. */
		.length = row_in_batch_count,
		.offset = 0,
		.null_count = 0,
		.n_buffers = 1,
		.n_children = column_count,
		.dictionary = NULL,
		/* Bookkeeping. */
		.release = &release_malloced_array
	};
	/*
	 * Allocate list of parent buffers.
	 */
	array->buffers = malloc(sizeof(void *) * array->n_buffers);
	/* No NULLs, parent null bitmap can be omitted. */
	array->buffers[0] = NULL;
	/*
	 * Allocate list of children arrays.
	 */
	size_t size = sizeof(struct ArrowArray *) * array->n_children;
	array->children = malloc(size);
	/*
	 * Initialize children arrays.
	 */
	for (int c = 0; c < array->n_children; c++) {
		size_t size = sizeof(struct ArrowArray);
		struct ArrowArray *child = array->children[c] = malloc(size);
		*child = (struct ArrowArray) {
			/* Data description. */
			.length = row_in_batch_count,
			.offset = 0,
			.null_count = -1,
			.n_buffers = 2,
			.n_children = 0,
			.dictionary = NULL,
			.children = NULL,
			/* Bookkeeping. */
			.release = &release_malloced_array
		};
		child->buffers = malloc(sizeof(void *) * child->n_buffers);
		/*
		 * TODO: nulls_size = (row_in_batch_count / CHAR_BIT) + padding
		 * to a length that is a multiple of 64 bytes.
		 */
		size_t nulls_size = 64;
		child->buffers[0] = aligned_alloc(64, nulls_size);
		uint8_t *null = (uint8_t *) child->buffers[0];
		for (int r = 0; r < nulls_size; r++) {
			if (c == 1)
				null[r] = 0xAA;
			else if (c == 2)
				null[r] = 0x55;
			else
				null[r] = 0x00;
		}
		/*
		 * The data buffer should be aligned to 64 bytes.
		 */
		size_t data_size = sizeof(uint32_t) * row_in_batch_count;
		child->buffers[1] = aligned_alloc(64, data_size);
		uint32_t *col = (uint32_t *) child->buffers[1];
		for (int r = 0; r < array->length; r++) {
			uint32_t val = (c + 1) * 10000 + row_processed + r;
			col[r] = val;
		}
	}

	row_processed += row_in_batch_count;
	return 0;
}

int
init_stream(struct ArrowArrayStream *stream)
{
	stream->get_schema = gen_schema;
	stream->get_next = gen_array_chunk;
	stream->release = release_stream;
	return 0;
}
