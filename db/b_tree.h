#ifndef TreeNode_h_
#define TreeNode_h_

#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <fstream>
using namespace std;
 
class TreeNode{
private:
	char indexCharKey[100];
	int leaf;
	//uint32_t partitionNum;
	int partitionNum;
	TreeNode *left;
	TreeNode *right;
public:
    TreeNode();
    void printfTree();
    void persistentB_Tree(ofstream* treeFile);
    void setLeafFlag(int n);
    void setIndexCharKey(char* key);
    void setPartition(int partition);
    TreeNode * getLeftChild();
    TreeNode * getRightChild();
    void insertNode(char* InsertedIndexKey, int addPartition);
    void rebuildTree(TreeNode *addNode,int* addFlag);
    int binaryTreeSereach(char* Key);
    void destroyTree();
};

#endif