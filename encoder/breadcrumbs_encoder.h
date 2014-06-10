
#ifndef __BREADCRUMBS_ENCODER_H__
#define __BREADCRUMBS_ENCODER_H__

#define DIE_ON_ERROR(msg)\
    do { perror(msg); exit(EXIT_FAILURE); } while (0)
#define DIE(msg, ...)\
    do { fprintf(stderr, msg, ##__VA_ARGS__); exit(EXIT_FAILURE); } while (0)

#endif /* __BREADCRUMBS_ENCODER_H__ */
