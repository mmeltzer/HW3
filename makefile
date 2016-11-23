all:
	gcc -w -g -o test_assign3_1 test_assign3_1.c record_mgr.c dberror.c expr.c test_expr.c storage_mgr.c  buffer_mgr_stat.c buffer_mgr.c