#include <dlfcn.h>
#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdbool.h>
#include "../arrow.h"

const int64_t column_count = 3;

struct column {
	struct ArrowSchema schema;
	struct ArrowArray array;
};

typedef void (*gen_columns_ptr)(struct column *col);

int main()
{
	void *h = dlopen("produce.so", RTLD_LAZY);
	if (h == NULL) {
		fprintf(stderr, "%s\n", dlerror());
		exit(EXIT_FAILURE);
	}
	/* Clear any existing error. */
	dlerror();

	gen_columns_ptr gen_columns = dlsym(h, "gen_columns");
	if (gen_columns == NULL) {
		fprintf(stderr, "%s\n", dlerror());
		exit(EXIT_FAILURE);
	}

	size_t size = column_count * sizeof(struct column);
	struct column *columns = malloc(size);

	for (int chunk_num = 0; true; chunk_num++) {
		gen_columns(columns);

		if (columns[0].array.length == 0)
			break;

		int64_t num_rows = columns[0].array.length;

		printf("\nchunk %d:\n", chunk_num);
		for (int r = 0; r < num_rows ; r++) {
			printf("[%d]\t", r);
			for (int c = 0; c < column_count; c++) {
				struct column *col = &columns[c];
				uint8_t *null = (uint8_t *) col->array.buffers[0];
				uint32_t *val = (uint32_t *) col->array.buffers[1];
				bool is_null = null[r / 8] & (1 << (r % 8));
				if (is_null)
					printf("null\t");
				else
					printf("%u\t", val[r]);
			}
			printf("\n");
		}

		for (int c = 0; c < column_count; c++) {
			struct column *col = &columns[c];
			col->array.release(&col->array);
			col->schema.release(&col->schema);
		}
	}

	free(columns);
	dlclose(h);
	return 0;
}
