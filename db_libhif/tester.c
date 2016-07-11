#include "hif_swdb.c"
#include <stdio.h>

int main()
{

  HifSwdb *swdb = hif_swdb_new();
  printf("%s\n",hif_swdb_get_path(swdb));
  if(hif_swdb_exist(swdb))
    printf("SWDB exists\n");
  else
    {
        printf("SWDB not present, creating new\n");
        printf("%d\n", hif_swdb_create_db (swdb));
    }

  //hif_swdb_reset_db(swdb);
  hif_swdb_add_group_package(swdb, 1, "PACKAGE");
  printf("%d\n", hif_swdb_get_package_type(swdb, "installed"));
  hif_swdb_finalize(swdb);
  return 0;
}
