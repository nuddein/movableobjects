from rtree import index
import streamlit as st
import rtreetest as rt
import streamlit.components.v1 as components
import matplotlib.pyplot as plt


def main():
    col1, col2, col3 = st.columns(3)

    st.title("Moving Objects Database Test UI") 
    with col1:
        settings_page()
    # Page to load objects
    st.header("Load Objects")
    mbr_siz = st.number_input("Enter MBR Size", step=0.0001, format="%0.3f", value=0.1)
    dataset_size = int(st.number_input("Enter Dataset Size", min_value=0, value=10, step=1))
    db_name = st.text_input("Enter Database Name", value = "test")

    if st.button("Load Objects"):
        modb = rt.MODB(dataset_size,mbr_siz,db_name)
        modb.loadObjects()
        modbr = rt.MODB_rtree_only(dataset_size,mbr_siz,db_name + "-r-only")
        modbr.loadObjects()
        rt.db_list.update ({db_name : modb})
        rt.rtree_only_db_list.update({db_name : modbr})
        st.write("Leaf Capacity of Rtree" , rt.p.leaf_capacity)
        st.write("Page Size of Rtree" , rt.p.pagesize)
        st.write("Buffering Capacity of Rtree" , rt.p.buffering_capacity)
        st.success("Objects loaded successfully!")

    st.header("Plot Area")
    key_plot = st.selectbox("Select DB ->", list(rt.db_list.keys()))
    range_plot = st.text_input("Enter Range", value="0.0,0.0,1.0,1.0")
    #key = 'test'
    
    if st.button("Plot Area"):
        db = rt.db_list[key_plot]
        db.plot(tuple([float(x) for x in range_plot.split(',')]))
        st.success("Plotted Successfully")


    # Page to create workload
    st.header("Create Workload with Plot")
    key = st.selectbox("Select DB      ", list(rt.db_list.keys()))
    #key = 'test'
    tmax = st.number_input("Enter TMax", min_value=0, value=10, step=1)
    range_freq = st.number_input("Enter Range Query Frequency (n Queries Every Time interval)", min_value=1, value=2,max_value=tmax)
    if st.button("Create Workload"):
        db = rt.db_list[key]
        db.createWorkLoadWithPlot(tmax,range_freq)
        st.success("Workload created successfully!")

    #st.header("Query Range")
    #dbname = st.selectbox("Select DB::", list(rt.db_list.keys()))
    #range = st.text_input("Enter Range", value="0.125,0.125,0.500,0.500")

    #if st.button("Query Range"):
    #    db = rt.db_list[dbname]
    #    result = db.query_range(tuple([float(x) for x in range.split(',')]))
       # st.write('Total False Hit Rate:', len(result[0]))

    st.header("Create Workload")
    
    keyd = st.selectbox("Select DB:", list(rt.db_list.keys()))
    #key = 'test'
    tmaxd = st.number_input("Enter TMax -> ", min_value=0, value=10, step=1)
    range_freqd = st.number_input("Enter Range Query Frequency (n Queries Every Time interval) ", min_value=1, value=2,max_value=tmax)
    movement_scalee = st.number_input("Enter Speed Scaling Factor (object can move + or - n units times MBR size)  ->", min_value=0.01, value=0.75,max_value=100.0)
    if st.button("Create Big Workload "):
        db = rt.db_list[keyd]
        db.createWorkLoad(tmaxd,range_freqd,movement_scalee)
        dbr = rt.rtree_only_db_list[keyd]
        dbr.createWorkLoad(tmaxd,range_freqd,movement_scalee)
        st.success("Workload ended successfully!")
        

    st.header("Benchmark")

    mbr_sizeb = st.number_input("Enter MBR Size for benchmark" , step=0.0001, format="%0.3f", value=0.1)
    tmaxb = st.number_input("Enter TMax for benchmark", min_value=0, value=10, step=1)
    dataset_sizesb = st.text_input("Enter dataset sizes for benchmark each seperated by a comma", value = "1000,2000,5000,10000")
    range_freqb = st.number_input("Enter Range Query Frequency (n Queries Every Time interval)  ", min_value=-1, value=2,max_value=100)
    movement_scale = st.number_input("Enter Speed Scaling Factor (object can move + or - n units times MBR size)  ", min_value=0.01, value=0.75,max_value=100.0)
    if st.button("Benchmark"):
        dset_sizes = dataset_sizesb.split(',')
        dset_sizes = [int(value) for value in dset_sizes]
        load_secs = dict()
        workload_secs = dict()
        workload_secs_rtree_only = dict()
        load_secs_rtree_only = dict()
        for size in dset_sizes:
            bench_dbname = "Benchmark-" + str(size)
            modb = rt.MODB(size,mbr_sizeb,bench_dbname)
            modbr = rt.MODB_rtree_only(size,mbr_sizeb,bench_dbname + "Rtree-only")
            secs = modb.loadObjects()
            secsr = modbr.loadObjects()
            rt.db_list.update ({bench_dbname : modb})
            rt.rtree_only_db_list.update({bench_dbname : modbr})
            load_secs.update({str(size) + "R": secs[0]})
            load_secs.update({str(size) + "B": secs[1]})
            load_secs_rtree_only.update({str(size): secsr})

        for size in dset_sizes:
            bench_dbname = "Benchmark-" + str(size)
            modb = rt.db_list[bench_dbname]
            modbr = rt.rtree_only_db_list[bench_dbname]
            secs = modb.createWorkLoad(tmaxb,range_freqb,movement_scale)
            secsr = modbr.createWorkLoad(tmaxb,range_freqb,movement_scale)
            workload_secs.update({str(size) + " T": secs[0]})
            workload_secs.update({str(size) + " R": secs[1]})
            workload_secs_rtree_only.update({str(size) + " T": secsr[0]})
            workload_secs_rtree_only.update({str(size) + " R": secsr[1]})
        #plot the data

        plot_bar_graph(load_secs,"Load Times (for Rtree and Btree respectively)")
        plot_bar_graph(workload_secs,"Workload Times (T for Total R for Range Query)")
        
        plot_bar_graph(load_secs_rtree_only,"Load Times for R-Tree only index")
        plot_bar_graph(workload_secs_rtree_only,"Workload Times for R-Tree only index")
def plot_bar_graph(data,title):
    # Extract keys and values from the dictionary
    x = list(data.keys())
    y = list(data.values())
    plt.clf()
    plt.tight_layout()
    plt.bar(x, y)
    plt.title(title)
    plt.xlabel('Dataset sizes')
    plt.ylabel('Runtime (seconds)')
    
    st.pyplot(plt)

def settings_page():
    st.title("Settings")
    
    # Define the range for the Leaf Capacity slider
    leaf_capacity_min = 10
    leaf_capacity_max = 1000
    
    # Define the range for the Fill Factor slider
    fill_factor_min = 0.1
    fill_factor_max = 0.99
    
    # Display the Leaf Capacity slider
    leaf_capacity = st.slider(
        "Leaf Capacity",
        min_value=leaf_capacity_min,
        value = 100,
        max_value=leaf_capacity_max,
        step=8
    )
    # Display the Fill Factor slider
    fill_factor = st.slider(
        "Fill Factor",
        min_value=fill_factor_min,
        value = 0.9,
        max_value=fill_factor_max,
        step=0.01
    )
    page_size = st.slider(
        "Page Size",
        min_value=8,
        value = 4096,
        max_value=8192,
        step=8
    )
    

    if st.button("Apply Settings "):
        rt.p.fill_factor = fill_factor
        rt.p.leaf_capacity = leaf_capacity
        rt.p.pagesize = page_size
        st.success("Settings Applied Successfully!")
    





if __name__ == "__main__":
    main()