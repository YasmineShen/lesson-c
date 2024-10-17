#include "crushit.h"
#include "mydefs.h"


bool initialise(state* s, const char* str)
{
/*if (s == NULL || str == NULL){
    return false;
}

FILE* fp = fopen(str,"r");//这里用r还是w

if(fp == NULL){
 int length = strlen(str);
 //str长度超过二维数组溢出，或者有不是英文字母的字符
 if(length != (HEIGHT * WIDTH)){
    return false;//还是exit(EXIT_FAILURE);
 }
 for (int i = 0; i < length; i++) {
    if (!isalpha(str[i])) {
        return false;
    }
}

 for(int i = 0; i < HEIGHT; i++){
    for(int j = 0; j < WIDTH; j++){
        //store each letter into the 2Darray
        s->board[i][j] = str[i * WIDTH + j];
    }
 }
}
else{
 fclose(fp);
}
 return true;*/
if (s == NULL || str == NULL) {
    return false;
}

// 尝试打开文件
FILE* fp = fopen(str, "r");
if (fp != NULL) {
    // 文件成功打开，从文件中读取数据
    for (int i = 0; i < HEIGHT; i++) {//////height 改maxrows
        for (int j = 0; j < WIDTH; j++) {
            int c = fgetc(fp);  // 从文件中读取一个字符
            if (c == EOF || !isalpha(c)) {  // 遇到文件结尾或非字母
                fclose(fp);
                return false;  // 文件格式不正确
            }
            s->board[i][j] = (char)c;
        }
        fgetc(fp);  // 跳过换行符
    }
    fclose(fp);
    return true;  // 成功从文件读取并初始化
} 
else {
    // 文件无法打开，假定 str 是棋盘的字符串
    int length = strlen(str);
    if (length != (HEIGHT * WIDTH)) {
        return false;  // 长度不符合要求
    }
    
    // 检查 str 中是否包含非字母的字符
    for (int i = 0; i < length; i++) {
        if (!isalpha(str[i])) {
            return false;
        }
    }

    // 从 str 中初始化棋盘
    for (int i = 0; i < MAXROWS; i++) {
        for (int j = 0; j < WIDTH; j++) {
            s->board[i][j] = str[i * WIDTH + j];
        }
    }
    return true;  // 成功从字符串初始化
}
}





bool tostring(const state* s, char* str)
{  
if (s == NULL || str == NULL){
    return false;
}

/*/k: The index of the string array
int  k = 0;
for(int i = 0; i < HEIGHT; i++){
    for(int j = 0; j < WIDTH; j++){
        str[k++] = s->board[i][j];
    }
}
str[k] = '\0';
return true;*/
int highestRow = -1;
for (int i = 0; i < HEIGHT; i++) {
    bool allDots = true;  // 假设当前行全是 '.'
    for (int j = 0; j < WIDTH; j++) {
        if (s->board[i][j] != '.') {
            allDots = false;  // 如果不是全是 '.'，更新标志
        }
    }
    if (!allDots) {
        highestRow = i;  // 如果当前行不是全是 '.'，更新最高有效行
    }
}

// 如果棋盘上没有有效字符，返回一个空字符串
if (highestRow == -1) {
    str[0] = '\0';
    return true;
}

// Step 2: 构建字符串，从最高有效行开始，跳过全是 '.' 的行
int k = 0;  // 字符串的索引
for (int i = 0; i <= MAXROWS; i++) {
    bool allDots = true;  // 假设当前行全是 '.'
    for (int j = 0; j < WIDTH; j++) {
        if (s->board[i][j] != '.') {
            allDots = false;  // 如果不是全是 '.'，更新标志
        }
    }

    if (!allDots) {  // 只处理非全 '.' 的行
        for (int j = 0; j < WIDTH; j++) {
            str[k++] = s->board[i][j];  // 将该行字符添加到字符串中
        }
    }
}

str[k] = '\0';  // 结束字符串
return true;
}


bool matches(state* s)
{
if (s == NULL){
    return false;
}



bool matchflag = false;
bool matched[MAXROWS][WIDTH] = {false};

// Check for vertical matches, from bottom to top
for (int j = 0; j < WIDTH; j++) {
    for (int i = MAXROWS - 1; i >= 2; i--) { // Start from the bottom row
        if (s->board[i][j] == s->board[i-1][j] &&
            s->board[i][j] == s->board[i-2][j] &&
            s->board[i][j] != '.' &&
            (i == 2 || s->board[i][j] != s->board[i-3][j])) {
            matchflag = true;
            matched[i][j] = matched[i-1][j] = matched[i-2][j] = true;
        }
    }
}

// Check for horizontal matches, from bottom to top
for (int i = MAXROWS - 1; i >= 0; i--) { // Start from the bottom row
    for (int j = WIDTH - 1; j >= 2; j--) { // Start from the rightmost column
        if (s->board[i][j] == s->board[i][j-1] &&
            s->board[i][j] == s->board[i][j-2] &&
            s->board[i][j] != '.' &&
            (j == 2 || s->board[i][j] != s->board[i][j-3])) {
            matchflag = true;
            matched[i][j] = matched[i][j-1] = matched[i][j-2] = true;
        }
    }
}

// Remove the matched letters by replacing them with '.'
for (int i = 0; i < MAXROWS; i++) {
    for (int j = 0; j < WIDTH; j++) {
        if (matched[i][j] == true) {
            s->board[i][j] = '.';
        }
    }
}
return matchflag; 
}



bool dropblocks(state* s)
{
if (s == NULL){
    return false;
}

/*bool removed = false;
int removedrow = HEIGHT - 1;

for(int j = 0; j < WIDTH; j++){
    for(int i = HEIGHT - 1; i >= 0; i-- ){
        if(s->board[i][j] != '.'){
            if (i != removedrow) {//啥意思
                s->board[removedrow][j] = s->board[i][j];
                s->board[i][j] = '.';
                removed = true;
            }
        removedrow--;
        }
    }
}
return true;*/
    bool removed = false;

    // 遍历每列
    for (int j = 0; j < WIDTH; j++) {
        int removedrow = HEIGHT - 1;  // 每列独立的“掉落”位置

        // 从底部往上遍历每列的方块
        for (int i = HEIGHT - 1; i >= 0; i--) {
            if (s->board[i][j] != '.') {  // 如果当前方块不是空位
                if (i != removedrow) {    // 需要移动块到最底端可用位置
                    s->board[removedrow][j] = s->board[i][j];
                    s->board[i][j] = '.';  // 清空原位置
                    removed = true;
                }
                removedrow--;  // 更新掉落位置，向上移一格
            }
        }
    }

    return removed;  // 返回是否有块被移动

}



void test(void)
{
   assert(initialise(NULL, "") == false);
   state s;
   assert(initialise(&s, NULL) == false);
   // Input string too short (not a multiple of 5)
   assert(initialise(&s, "ABCDBCDAACDAABDAABCAABCDABCDA") == false);
   // Incorrect character '+'
   assert(initialise(&s, "ABCDBCDAACDAABDAABCAABCDABCD+A") == false);
}

