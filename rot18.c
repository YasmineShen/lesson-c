#include "rot18.h"

#define OFFSET_ROT13 13
#define OFFSET_ROT5 5

void rot(char str[]) {
 for (int i = 0; str[i] != '\0'; i++) {
   if (isalpha(str[i])) {
     if (isupper(str[i])) {
       if (str[i] >= 'A' && str[i] <= 'M') {
          str[i] = str[i] + OFFSET_ROT13;
       } else if (str[i] >= 'N' && str[i] <= 'Z') {
          str[i] = str[i] - OFFSET_ROT13;
       }
     } else if (islower(str[i])) {
       if (str[i] >= 'a' && str[i] <= 'm') {
          str[i] = str[i] + OFFSET_ROT13;
       } else if (str[i] >= 'n' && str[i] <= 'z') {
          str[i] = str[i] - OFFSET_ROT13;
       }
     }
   } else if (isdigit(str[i])) {
      if (str[i] >= '0' && str[i] <= '4') {
          str[i] = str[i] + OFFSET_ROT5;
     } else if (str[i] >= '5' && str[i] <= '9'){ 
          str[i] = str[i] - OFFSET_ROT5;
     }
   }
 }
}


void your_tests(void){
  char str[100];
  strcpy(str, "Have a nice day!");
  rot(str);
  assert(strcmp(str, "Unir n avpr qnl!") == 0);
  rot(str);
  assert(strcmp(str, "Have a nice day!") == 0);
  strcpy(str, "0816");
  rot(str);
  assert(strcmp(str, "5361") == 0);
}