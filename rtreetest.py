import matplotlib.pyplot as plt
from rtree import index
from BTrees.IOBTree import IOBTree
import random
import persistent
import transaction
import sys
import os
import ZODB, ZODB.FileStorage
import streamlit as st
import time
from multiprocessing import Process, Lock


db_list = dict()
rtree_only_db_list = dict()

p = index.Property()
p.buffering_capacity = 10000

def setProperties(prop_map):
        for key,value in prop_map:
            if hasattr(index,key):
                setattr(index,key,value)

def setProp(prop,val):
    if hasattr(index,prop):
        print('Has Attr! ')
        setattr(index,prop,val)


class MODB:

    def __init__(self, n,mbr_siz,index_name):
            self.n = n
            self.mbr_size = mbr_siz   
            self.save_freq = 100000
            #Overwrite in case a file with the same name found
            if os.path.isfile(index_name + '-btree.data'):
                os.remove(index_name + '-btree.data')
            if os.path.isfile(index_name + '-rtree.data'):
                os.remove(index_name + '-rtree.data')
            self.storage = ZODB.FileStorage.FileStorage(index_name + '-btree.data')
            self.db = ZODB.DB(self.storage)
            self.connection = self.db.open()
            self.root = self.connection.root
            # Create an Rtree index
            self.idx = index.Index(index_name + '-rtree',properties = p)
            self.idx_name = index_name
            self.root.MovingObjects = IOBTree()
            self.knn_rate = 8
            self.lock = Lock()
            

    


    # Create btree based storage for storing MBRs and Moving Objects
    # 50 entry per page
    

    

    # Create a figure and a set of subplots

    # Create some random bounding boxes and add to the index

    def generateRandomMovementEquation(self):
        return ( random.uniform( -1 * self.velocity_range, self.velocity_range ), random.uniform(-1* self.acceleration_range, self.acceleration_range), random.uniform( -1 * self.velocity_range, self.velocity_range ), random.uniform(-1* self.acceleration_range, self.acceleration_range)    )
    def generateBoundingBox(self,dot,mbr_size):
        return (dot[0] - mbr_size / 2  , dot[1] - mbr_size / 2 ,dot[0] + mbr_size / 2  , dot[1] + mbr_size / 2  )
    

    class moving_object(persistent.Persistent):
        # x = x0 + v0x * t + 0.5*ax*t ** 2
        # y = y0 + v0y*t + 0.5*ay*t**2
        
        def moveObject(self,t): #default is 1                 
            return ( self.dot[0] + self.v0x * t + 0.5 * self.ax * t ** 2,self.dot[1] + self.v0y * t + 0.5 * self.ax * t ** 2)

        def moveRandom(self,mbr_size, range = 0.75):
            x = random.uniform(-1 * mbr_size * range,mbr_size * range)
            self.dot[0] += x 

            y = random.uniform(-1 * mbr_size * range,mbr_size * range)
            self.dot[1] += y 

        def setDot(self,dot):
            self.dot = dot
            
        def setBox(self,mbr_size):
            self.box = (self.dot[0] - mbr_size / 2  , self.dot[1] - mbr_size / 2 ,self.dot[0] + mbr_size / 2  , self.dot[1] + mbr_size / 2  )

        def serialize(self):
            return (self.dot , self.box)
        
        def deserialize(self,entry):
            self.setDot(entry[0])
            self.box = entry[1]

        def moveWithOffset(self,x_offset,y_offset):
            self.dot[0] += x_offset
            self.dot[1] += y_offset
        def moveRandomWithinTheBox(self):
            self.dot = random.uniform(self.box[0],self.box[2]) , random.uniform(self.box[1],self.box[3])

    class Dot(persistent.Persistent):
        
        def __init__(self,oid,dot):
            self.oid = oid
            self.dot = dot
        # to serialize only write the corresponding dot

        def moveRandom(self,mbr_size, range = 1):
            x = random.uniform(mbr_size * range)
            x_sign = -1
            if(x % 1 == 0):
                x_sign = 1
            self.dot[0] = x * x_sign
            
            y = random.uniform(mbr_size * range)
            y_sign = -1
            if(y % 1 == 0):
                y_sign = 1
            self.dot[1] = y * y_sign
            
    def loadObjects(self, start= 0,end= None):
        print('Args=', start,end)
        if end is None:
            end = self.n
        rtree_load_time = 0
        btree_load_time = 0

        for i in range(start,end):# i is the OID
            min_x = random.uniform(0, 1)
            min_y = random.uniform(0, 1)
            max_x = min_x +  self.mbr_size
            max_y = min_y + self.mbr_size   
        # m_obj = self.moving_object()
            start_secs = time.time()
            self.idx.insert(i, (min_x, min_y, max_x, max_y))
            end_secs = time.time()
            rtree_load_time = rtree_load_time + end_secs - start_secs
            x = (min_x + max_x) / 2
            y = (min_y + max_y) / 2
            dot = [x,y]
            #m_obj.setDot(dot)
            
        # m_obj.setBox(self.mbr_size)
            start_secs = time.time()
            self.root.MovingObjects[i] = (dot , (min_x,min_y,max_x,max_y))
            end_secs = time.time()
            btree_load_time = btree_load_time + end_secs - start_secs
        # self.root.MovingObjects[i] = m_obj.serialize()
            
            if (i+1) % self.save_freq == 0:
                start_secs = time.time()
                transaction.commit()
                end_secs = time.time()
                btree_load_time = btree_load_time + end_secs - start_secs
        start_secs = time.time()
        transaction.commit()
        end_secs = time.time()
        btree_load_time = btree_load_time + end_secs - start_secs
        st.write('Load Took ', rtree_load_time , ' seconds for Rtree' , btree_load_time , 'Seconds for BTree')
        return (rtree_load_time,btree_load_time)

    batch_count = 100

    def createWorkLoadWithPlot(self,tmax,range_query_freq = 2):
        range_query_time = 0
        for t in range(tmax):
            fig, ax = plt.subplots()
            for j in self.root.MovingObjects:
                #print(i, 'iteration
                
                m_obj = self.moving_object()
                m_obj.deserialize(self.root.MovingObjects[j])
                 # get initial dot
                dot_x,dot_y = m_obj.dot
                intersections = self.idx.intersection((dot_x,dot_y,dot_x,dot_y),objects = True) # get all intersecting bounding boxes with the given OID
                intersect_dict = dict()
                ids = []
                for item in intersections:
                    intersect_dict[item.id] = item.bbox
                ids = intersect_dict.keys()
                #min_x, min_y, max_x, max_y = m_obj.box[0], m_obj.box[1], m_obj.box[2], m_obj.box[3] 
                
                
                #print('intersections',intersections, 'j' , j)


                if int(j) not in ids: 
                    #print('object with id ', j ,'is missed its bounding box recreating a new mbr')
                    new_mbr = ( dot_x - self.mbr_size /2,dot_y - self.mbr_size /2 , dot_x + self.mbr_size /2 , dot_y + self.mbr_size /2)
                    self.idx.delete(j,m_obj.box)
                    self.idx.insert(j,new_mbr)
                    m_obj.box = new_mbr
                    #min_x, min_y, max_x, max_y = m_obj.box[0], m_obj.box[1], m_obj.box[2], m_obj.box[3]                   
                    min_x, min_y, max_x, max_y = new_mbr
                    # Create a rectangle patch for representing newly created bounding boxes
                    patch2 = plt.Rectangle((min_x, min_y), max_x - min_x, max_y - min_y,fill=False, edgecolor='green', linewidth=1)
                    ax.add_patch(patch2)

                else:
                    #min_x, min_y, max_x, max_y = self.getBoundingBox(j,intersections)
                    # Create a rectangle patch
                    min_x, min_y, max_x, max_y = intersect_dict[j]
                    patch = plt.Rectangle((min_x, min_y), max_x - min_x, max_y - min_y,fill=False, edgecolor='blue', linewidth=1)
                    ax.add_patch(patch)

                    
                plt.scatter(dot_x, dot_y, color='red', s=0.05)
                
                # Add the patch to the Axes
                
                #Save Object's new location to the btree
                m_obj.moveRandom(self.mbr_size)
                self.root.MovingObjects[j] = m_obj.serialize()
                
                #update the dot
                
            # Show the plot
            transaction.commit()
            colors = ['red', 'blue']
            descriptions = ['Real Location', 'MBR' ]
            if(t % range_query_freq == 0):
                # query a random given range
                min_x, min_y  = random.uniform(0,1),random.uniform(0,1)
                max_x, max_y = random.uniform(min_x,1),random.uniform(min_y,1)
                res = self.query_range((min_x, min_y, max_x, max_y ))
                range_query_time += res[1]
                
                patch = plt.Rectangle((min_x, min_y), max_x - min_x, max_y - min_y,fill=False, edgecolor='yellow', linewidth=1)
                ax.add_patch(patch)
                colors.append('yellow')
                descriptions.append('Range Query')
            
            ax.set_aspect('equal', 'box')
            min_x, min_y, max_x, max_y  = 0,0,1,1
            ax.set_xlim(min_x, max_x)
            ax.set_ylim(min_y, max_y)
            

            legend_handles = [plt.Line2D([], [], color=color, marker='o', linestyle='None') for color in colors]

            plt.legend(handles=legend_handles, labels=descriptions, loc='lower left',mode="expand",ncol=3,bbox_to_anchor=(0,1.02,1,0.2), fontsize='small')

            
            ax.set_title('At t=' + str(t))
            st.pyplot(fig)
        
            #plt.savefig('t=' + str(t) + '.png', dpi=800)
        st.write('Range Queries Took ', range_query_time, ' seconds')
            #plt.show()
 
    def plot(self,range=(0,0,1,1)):
        
        fig, ax = plt.subplots()
        for j in self.root.MovingObjects:

            m_obj = self.moving_object()
            m_obj.deserialize(self.root.MovingObjects[j])
            dot_x, dot_y = m_obj.dot
            intersections = self.idx.intersection((dot_x,dot_y,dot_x,dot_y),objects = True) # get all intersecting bounding boxes with the given OID
            intersect_dict = dict()
            for item in intersections:
                intersect_dict[item.id] = item.bbox
            min_x, min_y, max_x, max_y = intersect_dict[j]
            patch = plt.Rectangle((min_x, min_y), max_x - min_x, max_y - min_y,fill=False, edgecolor='blue', linewidth=1)
            ax.add_patch(patch)  
            plt.scatter(dot_x, dot_y, color='red', s=0.05)
        colors = ['red', 'blue']
        descriptions = ['Real Location', 'MBR']
        min_x, min_y, max_x, max_y = range
        range_patch = plt.Rectangle((min_x, min_y), max_x - min_x, max_y - min_y,fill=False, edgecolor='yellow', linewidth=1)
        ax.add_patch(range_patch)  
        if range != (0,0,1,1):
            colors.append('yellow')
            descriptions.append('Range Query')
            res = self.query_range(range)
            st.write("False Hits:", res[0])
            st.write("Range Query Took", res[1], 'seconds')


        legend_handles = [plt.Line2D([], [], color=color, marker='o', linestyle='None') for color in colors]
        plt.legend(handles=legend_handles, labels=descriptions, loc='lower left',mode="expand",ncol=3,bbox_to_anchor=(0,1.02,1,0.2), fontsize='small')

        ax.set_aspect('equal', 'box')
        min_x, min_y, max_x, max_y  = 0,0,1,1
        ax.set_xlim(min_x, max_x)
        ax.set_ylim(min_y, max_y)
        

        transaction.commit()
        st.pyplot(fig)

    # This workload does not require writing to btree index btree index only serves as a place for holding the initial locations of objects
    def createWorkLoad(self,tmax,range_query_freq,movement_scale):
         tick = 0
         print("range query freq is" , range_query_freq)
         range_query_time = 0
         start = time.time()
         for t in range(tmax):
            for j in self.root.MovingObjects:
                m_obj = self.moving_object()
                m_obj.deserialize(self.root.MovingObjects[j]) # get initial dot
                
                dot_x,dot_y = m_obj.dot
                 # get all intersecting bounding boxes with the given OID
                intersections = self.idx.intersection((dot_x,dot_y,dot_x,dot_y))
                #min_x, min_y, max_x, max_y = m_obj.box[0], m_obj.box[1], m_obj.box[2], m_obj.box[3] 

                #print('intersections',intersections, 'j' , j)
                if int(j) not in list(intersections):
                    self.idx.delete(j,m_obj.box)
                    new_mbr = ( dot_x - self.mbr_size /2,dot_y - self.mbr_size /2 , dot_x + self.mbr_size /2 , dot_y + self.mbr_size /2)
                    m_obj.box = new_mbr
                    self.idx.insert(j,new_mbr)
                    #min_x, min_y, max_x, max_y = m_obj.box[0], m_obj.box[1], m_obj.box[2], m_obj.box[3]
                
                m_obj.moveRandom(self.mbr_size,movement_scale)
                self.root.MovingObjects[j] = m_obj.serialize() #.serialize()
                if (t + 1) % range_query_freq == 0 and range_query_freq > 0.0:
                # query a random given range
                    min_x, min_y  = random.uniform(0,1),random.uniform(0,1)
                    max_x, max_y = random.uniform(min_x,1),random.uniform(min_y,1)
                    res = self.query_range((min_x, min_y, max_x, max_y ), verbose=True)
                    range_query_time += res[1]
                
                tick = tick + 1
                if(tick % self.save_freq == 0):
                    transaction.commit()

         end = time.time()
         st.write("Workload took ", end - start, " seconds")
         st.write("Range Queries took", range_query_time , "seconds" )
         print('pass', t)
         return (end-start,range_query_time)
    
           
    #gives bounding_box and dot location    
    def query_object(self,oid):
        start = time.time ()
        m_obj = self.moving_object()
        m_obj.deserialize(self.root.MovingObjects[oid])
        dot_x,dot_y = m_obj.dot
        intersections = self.idx.intersection((dot_x,dot_y,dot_x,dot_y),objects = True)
        if intersections == []:
            st.Error('Cannot Find Object With Id ', oid)
            return (m_obj.dot,[])
        box = self.getBoundingBox(oid,intersections)
        end = time.time()
        st.write("Query took ", end - start , "seconds")
        return (m_obj.dot,box)
    
    
    def query_range(self,range,verbose = False):
        start = time.time ()
        false_hits = []
        
        intersections = list(self.idx.intersection(range))
        if verbose == False:
            st.write('Intersections', intersections)
        for oid in intersections:
            x,y = self.root.MovingObjects[oid][0]
            if not (range[0] <= x <= range[2] and range[1] <= y <= range[3]):
                false_hits.append(oid)
        end = time.time()
        if verbose == False:
            st.write("Range Query took ", end - start , "seconds")
            st.write("False Hits ->", len(false_hits), "\n" ,false_hits)
        return (false_hits,end-start)


#  Class for index that only uses RTree 
#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
class MODB_rtree_only:

    def __init__(self, n,mbr_siz,index_name):
            self.n = n
            self.mbr_size = mbr_siz   
            self.save_freq = 1000
            #Overwrite in case a file with the same name found
            if os.path.isfile(index_name + '-rtree-only.data'):
                os.remove(index_name + '-rtree-only.data')
            # Create an Rtree index 
            self.idx = index.Index(index_name + '-rtree',properties = p)
            self.idx_name = index_name
            self.knn_rate = 8
            self.lock = Lock()
            
  
    def moveRandom(self,mbr_size, range = 0.75):
            x = random.uniform(-1 * mbr_size * range,mbr_size * range)
        
            y = random.uniform(-1 * mbr_size * range,mbr_size * range)
            return x,y
    

    # Create a figure and a set of subplots

    # Create some random bounding boxes and add to the index
            
    def loadObjects(self, start= 0,end= None):
        print('Args=', start,end)
        if end is None:
            end = self.n
        rtree_load_time = 0

        for i in range(start,end):# i is the OID
            min_x = random.uniform(0, 1)
            min_y = random.uniform(0, 1)
            max_x = min_x +  self.mbr_size
            max_y = min_y + self.mbr_size   
            x = (min_x + max_x) / 2
            y = (min_y + max_y) / 2
            start_secs = time.time()
            self.idx.insert(i, (x,y,x,y))
            end_secs = time.time()
            rtree_load_time = rtree_load_time + end_secs - start_secs
            
        st.write('Load Took ', rtree_load_time , ' seconds for Rtree only index')
        return rtree_load_time

    batch_count = 100


    # Iterate over each rectangle in the index

    # This workload does not require writing to btree index btree index only serves as a place for holding the initial locations of objects
    def createWorkLoad(self,tmax,range_query_freq = 2,movement_scale = 0.75):
         range_query_time = 0
         start = time.time()
         for t in range(tmax):
            bbox_dict = {item.id: item.bbox for item in self.idx.intersection(self.idx.bounds, objects=True)}
            for i in range(0,self.n):
                dot_x,dot_y = bbox_dict[int(i)][0],bbox_dict[int(i)][1]
                #min_x, min_y, max_x, max_y = m_obj.box[0], m_obj.box[1], m_obj.box[2], m_obj.box[3] 

                #print('intersections',intersections, 'j' , j)
                    #min_x, min_y, max_x, max_y = m_obj.box[0], m_obj.box[1], m_obj.box[2], m_obj.box[3]
                
                new_x,new_y = self.moveRandom(self.mbr_size,movement_scale)
                new_x += dot_x
                new_y += dot_y
                
                #update new dot's location in rtree
                self.idx.delete(i,(dot_x,dot_y,dot_x,dot_y))
                self.idx.insert(i,(new_x,new_y,new_x,new_y))


                if (t + 1) % range_query_freq == 0 and range_query_freq > 0.0:
                # query a random given range
                    min_x, min_y  = random.uniform(0,1),random.uniform(0,1)
                    max_x, max_y = random.uniform(min_x,1),random.uniform(min_y,1)
                    res = self.query_range((min_x, min_y, max_x, max_y ), verbose=True)
                    range_query_time += res[1]

         end = time.time()
         st.write("Workload took ", end - start, " seconds for RTree only index")
         st.write("Range Queries took", range_query_time , "seconds for RTree only index" )
         return (end-start,range_query_time)

    def query_range(self,rangee,verbose = False):
        start = time.time()
        inside = []
        
        intersections = list(self.idx.intersection(rangee))
        if verbose == False:
            st.write('Intersections', intersections)
        for oid in range(0, self.n):
            if oid in intersections:
                inside.append(oid)
        end = time.time()
        if verbose == False:
            st.write("Range Query took ", end - start , "seconds for Rtree only index") 

        return (inside,end-start)
