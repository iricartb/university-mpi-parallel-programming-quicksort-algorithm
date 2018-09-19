#include <stdio.h>
#include <assert.h>
#include <mpi.h>
#include <stdlib.h>

#define N 320000000

int * valors;
int * valors2;

void qs(int ii, int fi) {
   int i, f, j;
   int pivot, vtmp, vfi;

   pivot = valors[ii];
   i = ii + 1;
   f = fi;
   vtmp = valors[i];

   while (i <= f) {
      if (vtmp < pivot) {
         valors[i - 1] = vtmp;
         i++;
         vtmp = valors[i];
      }
      else {
         vfi = valors[f];
         valors[f] = vtmp;
         f--;
         vtmp = vfi;
      }
   }
   valors[i - 1] = pivot;

   if (ii < f) qs(ii, f);
   if (i < fi) qs(i, fi);
}

void merge(int * v1, int len1, int * v2, int len2, int * result) {
   int i, j;
   int index = 0;
   i = 0;
   j = 0;
  
   while ((i < len1) && (j < len2)) {
      if (v1[i] < v2[j]) {
         result[index] = v1[i];
         index++;
         i++;
      } else {
         result[index] = v2[j];
         index++;
         j++;
      }
   }
  
   while (i < len1) {
      result[index] = v1[i];
      index++;
      i++;
   }
  
   while (j < len2) {
      result[index] = v2[j];
      index++;
      j++;
   }
}

int main(int num_args, char * args[]) {
   int i, begin, end, quantity, c, cont, residuo;
   int numProcs, idProc;
   long long sum = 0;
   
   MPI_Init(&num_args, &args); 
   MPI_Comm_rank(MPI_COMM_WORLD, &idProc);
   MPI_Comm_size(MPI_COMM_WORLD, &numProcs);
   
   MPI_Status status;
   MPI_Request request[numProcs];
   
   quantity = N / numProcs;
   residuo = N - (quantity * numProcs);
   
   if ((numProcs == 4) || (numProcs == 8) || (numProcs == 2)) {
      if (idProc == 0) {
   	   valors = (int *) malloc(sizeof(int) * N);
   	   valors2 = (int *) malloc(sizeof(int) * N);
   	} 
      else valors = (int *) malloc(sizeof(int) * quantity);
   	  
   	end = (idProc * quantity) + quantity;
      begin = idProc * quantity;
      
      for (i=0; (i < end); i++) {
   	   if (i >= begin) valors[i - begin] = rand();
   	   else rand();
      }
      
      qs(0, (quantity - 1));
   	   if (idProc == 0) {
   	  	   begin = quantity;
   	  	   
            for (i=0; (i < (numProcs - 1)); i++) {
   	  	 	MPI_Irecv(&valors[begin], quantity, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &request[i]);
   	  	 	begin += quantity;
   	  	}
   	  	 
         begin = quantity;
   	  	cont = 0;
          
   	  	for (i=0; (i < (numProcs - 1)); i++) {
   	  	 	MPI_Wait(&request[i], &status);
   	  	 	
            if ((cont % 2) == 0) merge(&valors[0], begin, &valors[begin], quantity, &valors2[0]);
   	  	 	else merge(&valors2[0], begin, &valors[begin], quantity, &valors[0]);
            
   	  	   begin += quantity;
   	  	   cont++;
   	  	}
   	  	 
         sum = valors2[0];
   	  	for (i=1; (i < N); i++) {
	   	   assert(valors2[i - 1] <= valors2[i]);
	   	   if ((i % 100) == 0) sum += valors2[i];
         }
		 
         printf("validacio %ld \n", sum);
		   free(valors);
   	  	free(valors2);
   	} else {
   	   MPI_Send(&valors[0], quantity, MPI_INT, 0, 0, MPI_COMM_WORLD);
   	   free(valors);
      }
   } else if ((numProcs == 24) || (numProcs == 12)) {
      if (((idProc % 3) == 0) && (idProc != 0)) {
   	   valors = (int *) malloc(sizeof(int) * (quantity * 3));
   	   valors2 = (int *) malloc(sizeof(int) * (quantity * 3));
      } else if (idProc == 0) {
   	   valors = (int *) malloc(sizeof(int) * N);
   	   valors2 = (int *) malloc(sizeof(int) * N);
      } else valors = (int *) malloc(sizeof(int) * quantity);
   	  
   	end = ((idProc * quantity) + residuo) + quantity;
      if (idProc>0) begin = (idProc * quantity) + residuo;
      else begin = 0;
      
      for (i=0; (i < end); i++) {
   	   if (i >= begin) valors[i - begin] = rand();
   	   else rand();
      }
      
      if (idProc == 0) qs(0, ((quantity + residuo) - 1));
      else qs(0, (quantity - 1));
      
   	if ((idProc % 3) == 0) {
   	   cont = 0;
   	  	if (idProc == 0) begin = quantity + residuo;
   	  	else begin = quantity;
   	  	 
   	  	for (i=0; (i < 2); i++) {
   	  	   if ((cont % 2) == 0) {
   	  	 	   MPI_Recv(&valors[begin], quantity, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, &status);
   	  	 	   merge(&valors[0], begin, &valors[begin], quantity, &valors2[0]);
   	  	   } else {
   	  	 	   MPI_Recv(&valors2[begin], quantity, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, &status);
   	  	 	   merge(&valors2[0], begin, &valors2[begin], quantity, &valors[0]);
   	  	   }
            
   	      begin += quantity;
   	      cont++;
   	  	}
   	  	 
   	  	if (idProc == 0) {
   	  	   end = quantity*3;
   	  	 	for (i=0; (i < (numProcs / 3) - 1); i++) {
   	  	 	   MPI_Irecv(&valors[begin], end, MPI_INT, MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, &request[i]);
   	  	 	   begin += end;
   	  	   }
            
   	  	   cont = 0;
   	  	   begin = end + residuo;
   	  	   for (i=0; (i < ((numProcs / 3) - 1)); i++) {
   	  	 	   MPI_Wait(&request[i],&status);
   	  	 	   if ((cont % 2) == 0) {
   	  	 	      merge(&valors[0], begin, &valors[begin], end, &valors2[0]);
   	  	 	   } else {
   	  	 	      merge(&valors2[0], begin, &valors[begin], end, &valors[0]);
   	  	 	   }
   	  	      
               begin += end;
   	  	      cont++;
   	  	   }
            
   	  	   sum = valors2[0];
   	  	   for (i=1; (i < N); i++) {
	   	      assert(valors2[i - 1] <= valors2[i]);
	   	      if ((i % 100) == 0) sum += valors2[i];
	   	   }

            printf("validacio %ld \n",sum);
		      free(valors);
   	  	   free(valors2);
   	  	} else {
   	  	   MPI_Send(&valors[0], (quantity * 3), MPI_INT, 0, 2, MPI_COMM_WORLD);
   	  	   free(valors);
   	  	   free(valors2);
   	  	}
   	} else {
   	  	MPI_Send(&valors[0], quantity, MPI_INT, (idProc - (idProc % 3)), 1, MPI_COMM_WORLD);
   	  	free(valors);
   	}
   } else if (numProcs == 16) {
      if (((idProc % 2) == 0) && (idProc != 0)) {
   	   valors = (int *) malloc(sizeof(int) * quantity * 2);
   	   valors2 = (int *) malloc(sizeof(int) * quantity * 2);
      } else if (idProc==0) {
   	   valors = (int *) malloc(sizeof(int) * N);
   	   valors2 = (int *) malloc(sizeof(int) * N);
      } else valors = (int *) malloc(sizeof(int) * quantity);
   	  
   	end = (idProc * quantity) + quantity;
      begin = idProc * quantity;
      for (i=0; (i < end); i++) {
   	   if (i >= begin) valors[i - begin] = rand();
   	   else rand();
      }
      
      qs(0, (quantity - 1));
   	if ((idProc % 2) == 0) {
   	   cont = 0;
   	  	begin = quantity;
   	  	for (i=0; (i < 1); i++) {
   	  	   if ((cont % 2) == 0) {
   	  	 	   MPI_Recv(&valors[begin], quantity, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, &status);
   	  	 	   merge(&valors[0], begin, &valors[begin], quantity, &valors2[0]);
   	  	   } else {
   	  	 	   MPI_Recv(&valors2[begin], quantity, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, &status);
   	  	 	   merge(&valors2[0], begin, &valors2[begin], quantity, &valors[0]);
   	  	   }
   	      
            begin += quantity;
   	      cont++;
   	  	}
   	  	 
         if (idProc == 0) {
   	  	   end = quantity * 2;
   	  	 	for (i=0; (i < 7); i++) {
   	  	 	   MPI_Irecv(&valors2[begin], end, MPI_INT, MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, &request[i]);
   	  	 	   begin += end;
   	  	   }
   	  	   
            cont = 0;
   	  	   begin = end;
   	  	   for (i=0; (i < 7); i++) {
   	  	 	   MPI_Wait(&request[i], &status);
   	  	 	   if ((cont % 2) == 0) {
   	  	 	      merge(&valors2[0], begin, &valors2[begin], end, &valors[0]);
   	  	 	   } else {
   	  	 	      merge(&valors[0], begin, &valors2[begin], end, &valors2[0]);
   	  	 	   }
   	  	       begin += end;
   	  	       cont++;
   	  	   }
   	  	    
            sum = valors[0];
   	  	   for (i=1; (i < N); i++) {
	   	      assert(valors[i-1] <= valors[i]);
	   	      if ((i % 100) == 0) sum += valors[i];
	   	   }
            
		      printf("validacio %ld \n", sum);
		      free(valors);
   	  	   free(valors2);
   	  	} else {
   	  	   MPI_Send(&valors2[0], (quantity * 2), MPI_INT, 0, 2, MPI_COMM_WORLD);
   	  	   free(valors);
   	  	   free(valors2);
   	  	}
   	} else {
   	   MPI_Send(&valors[0], quantity, MPI_INT, (idProc - (idProc % 2)), 1, MPI_COMM_WORLD);
   	  	free(valors);
      }
   } else if ((numProcs == 32) || (numProcs == 64) || (numProcs == 128)) {
   	if (((idProc % 4) == 0) && (idProc < 32) && (idProc != 0)) {
   	   valors = (int *) malloc(sizeof(int) * quantity * (numProcs / 8));
   	   valors2 = (int *) malloc(sizeof(int) * quantity * (numProcs / 8));
      } else if (idProc==0) {
   	   valors = (int *) malloc(sizeof(int) * N);
   	   valors2 = (int *) malloc(sizeof(int) * N);
      } else valors = (int *) malloc(sizeof(int) * quantity);
   	  
   	end = (idProc * quantity) + quantity;
      begin = idProc * quantity;
      for (i=0; (i < end); i++) {
   	   if (i >= begin) valors[i - begin] = rand();
   	   else rand();
      }
      
      qs(0, (quantity - 1));
      
   	if (((idProc % 4) == 0) && (idProc < 32)) {
   	   cont = 0;
   	  	begin = quantity;
   	  	for (i=0; (i < ((numProcs / 8) - 1)); i++) {
   	  	   if (cont % 2 == 0) {
   	  	 	   MPI_Recv(&valors[begin], quantity, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, &status);
   	  	 	   merge(&valors[0], begin, &valors[begin], quantity, &valors2[0]);
   	  	   } else {
   	  	 	   MPI_Recv(&valors2[begin], quantity, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, &status);
   	  	 	   merge(&valors2[0], begin, &valors2[begin], quantity, &valors[0]);
   	  	   }
   	      
            begin += quantity;
   	      cont++;
   	  	}
         
   	  	if (idProc == 0) {
   	  	   end = quantity * (numProcs / 8);
   	  	 	for (i=0; (i < 7); i++) {
   	  	 	   MPI_Irecv(&valors2[begin], end, MPI_INT, MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, &request[i]);
   	  	 	   begin += end;
   	  	   }
            
   	  	   cont = 0;
   	  	   begin = end;
   	  	   for (i=0; (i < 7); i++) {
   	  	 	   MPI_Wait(&request[i], &status);
   	  	 	   if ((cont % 2) == 0) {
   	  	 	      merge(&valors2[0], begin, &valors2[begin], end, &valors[0]);
   	  	 	   } else {
   	  	 	      merge(&valors[0], begin, &valors2[begin], end, &valors2[0]);
   	  	 	   }
   	  	      
               begin += end;
   	  	      cont++;
   	  	   }
   	  	    
            sum = valors[0];
   	  	   for (i=1;i<N;i++) {
	   	      assert(valors[i-1] <= valors[i]);
	   	      if ((i % 100) == 0) sum += valors[i];
	   	   }
		      
            printf("validacio %ld \n",sum);
		      free(valors);
   	  	   free(valors2);
   	  	} else {
   	  	   MPI_Send(&valors2[0], quantity * (numProcs / 8), MPI_INT, 0, 2, MPI_COMM_WORLD);
   	  	   free(valors);
   	  	   free(valors2);
   	  	}
   	} else {
   	  	 if (idProc < 32) MPI_Send(&valors[0], quantity, MPI_INT, idProc - (idProc % 4), 1, MPI_COMM_WORLD);
   	  	 else if (idProc < 64) MPI_Send(&valors[0], quantity, MPI_INT, (idProc-(idProc % 4)) - 32, 1, MPI_COMM_WORLD);
   	  	 else if (idProc < 96) MPI_Send(&valors[0], quantity, MPI_INT, (idProc-(idProc % 4)) - 64, 1, MPI_COMM_WORLD);
   	  	 else MPI_Send(&valors[0], quantity, MPI_INT, (idProc - (idProc % 4)) - 96, 1, MPI_COMM_WORLD);
   	  	 free(valors);
   	}
   }

   MPI_Finalize();
   return 0;
}