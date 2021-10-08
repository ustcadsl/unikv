
#include <iostream>
#include <stdlib.h>
#include <string.h>
#include"db/b_tree.h"
 
TreeNode::TreeNode(){
    leaf=0;
    partitionNum=0;
    left=NULL;
    right=NULL;
}

void TreeNode::printfTree(){
	if(!this->leaf){
		printf("indexCharKey:%s\n",this->indexCharKey);
		this->left->printfTree();
		this->right->printfTree();
	}else{
		printf("leaf node,partition:%d\n",this->partitionNum);
	}
}
void TreeNode::persistentB_Tree(ofstream* treeFile){
  if(!this->leaf){
		//printf("indexCharKey:%s\n",this->indexCharKey);
		(*treeFile)<<"k:"<<this->indexCharKey<<"\n";
		this->left->persistentB_Tree(treeFile);
		this->right->persistentB_Tree(treeFile);
	}else{
		//printf("leaf node,partition:%d\n",this->partitionNum);
		(*treeFile)<<"p:"<<this->partitionNum<<"\n";
	  
	}
}

void  TreeNode::setLeafFlag(int n){
    this->leaf=n;
}

void TreeNode::setIndexCharKey(char* key){
    strcpy(this->indexCharKey,key);
}

void TreeNode::setPartition(int partition){
  this->partitionNum=partition;
}

TreeNode * TreeNode::getLeftChild(){
    return this->left;
}

TreeNode * TreeNode::getRightChild(){
    return this->right;
}

void TreeNode::rebuildTree(TreeNode *addNode, int* addFlag){
  if(!addNode->leaf){
     if(strcmp(addNode->indexCharKey,this->indexCharKey)<0){
       //printf("addNode->indexCharKey:%s < this->key:%s\n",addNode->indexCharKey,this->indexCharKey);
       if(this->left!=NULL){
	  this->left->rebuildTree(addNode,addFlag);
       }else{
	  this->left=addNode;
	  //printf(" this->key:%s,left:%s\n",this->indexCharKey,addNode->indexCharKey);
	}
      }else{
	//printf("addNode->indexCharKey:%s >= this->key:%s\n",addNode->indexCharKey,this->indexCharKey);
	 if(this->right!=NULL){
	    this->right->rebuildTree(addNode,addFlag);
	  }else{
	    this->right=addNode;
	    //printf(" this->key:%s,right:%s\n",this->indexCharKey,addNode->indexCharKey);
	  }
      }
  }else{
      if(this->left!=NULL && !this->left->leaf){
	  this->left->rebuildTree(addNode,addFlag);
      }
      if(this->left==NULL){
	this->left=addNode;
	*addFlag=1;
	//printf(" this->key:%s,left partition:%d\n",this->indexCharKey,addNode->partitionNum);
	return;
      }
      if(*addFlag==0){
	  if(this->right!=NULL && !this->right->leaf){
	      this->right->rebuildTree(addNode,addFlag);
	  }
	  if(this->right==NULL && *addFlag==0){
	    this->right=addNode;
	    *addFlag=1;
	    //printf(" this->key:%s,right partition:%d\n",this->indexCharKey,addNode->partitionNum);
	    return;
	  }
      }
  }
  
}

void TreeNode::insertNode(char* InsertedIndexKey, int addPartition){
    if (this->left==NULL && this->right==NULL) {   
        //printf("build root node:%d\n",addPartition);
	 strcpy(this->indexCharKey,InsertedIndexKey);
	 this->leaf=0;
	 TreeNode *addLeftNode=new TreeNode();
	  addLeftNode->leaf=1;
	  addLeftNode->partitionNum=addPartition-1;
		
	  TreeNode *addRightNode=new TreeNode();
	  addRightNode->leaf=1;
	  addRightNode->partitionNum=addPartition;
			           
	  this->left = addLeftNode;
	  this->right = addRightNode;
    } 
    else { 
		//insert node in left subtree 
		if(strcmp(InsertedIndexKey,this->indexCharKey)<0){
			if(this->left->leaf){
				TreeNode *addLeafNode=new TreeNode();
				addLeafNode->leaf=1;
				addLeafNode->partitionNum=addPartition;
				
				TreeNode *addIndexNode=new TreeNode();
				strcpy(addIndexNode->indexCharKey,InsertedIndexKey);
				addIndexNode->leaf=0;
				addIndexNode->left=this->left;
				addIndexNode->right=addLeafNode;   
				this->left=addIndexNode;////
			}else{
				this->left->insertNode(InsertedIndexKey,addPartition);
			}
		}else {
			if(this->right->leaf){
				TreeNode *addLeafNode=new TreeNode();
				addLeafNode->leaf=1;
				addLeafNode->partitionNum=addPartition;
				
				TreeNode *addIndexNode=new TreeNode();
				strcpy(addIndexNode->indexCharKey,InsertedIndexKey);
				addIndexNode->leaf=0;
				addIndexNode->left=this->right;////
				addIndexNode->right=addLeafNode;   
				this->right=addIndexNode;////
			}else{
				this->right->insertNode(InsertedIndexKey,addPartition);
			}
		} 
    } 
} 

int TreeNode::binaryTreeSereach(char* Key){
    if(this->leaf){
		return this->partitionNum;
    }else{
	if(strcmp(Key,this->indexCharKey)<0){
		//printf("Key:%s < indexCharKey:%s\n",Key,this->indexCharKey);
	    return this->left->binaryTreeSereach(Key);
	  }else{
	  	//printf("Key:%s >= indexCharKey:%s\n",Key,this->indexCharKey);
	    return this->right->binaryTreeSereach(Key);
	  }
    }
}

void TreeNode::destroyTree(){
    if(this->leaf){		
		//printf("delete leaf node,partition:%d\n",this->partitionNum);
		delete this;
    }else{
		this->left->destroyTree();
		this->right->destroyTree();
		//printf("delete index node,key:%s\n",this->indexCharKey);
		delete this;
    }
}
