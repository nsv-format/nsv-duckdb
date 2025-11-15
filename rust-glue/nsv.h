#ifndef NSV_H
#define NSV_H

#include <stddef.h>

typedef struct {
    char ***rows;
    size_t nrows;
    size_t *ncols;
    char *error;
} CNsvResult;

#ifdef __cplusplus
extern "C" {
#endif

CNsvResult* nsv_parse_file(const char *filename);
void nsv_free_result(CNsvResult *result);

#ifdef __cplusplus
}
#endif

#endif
