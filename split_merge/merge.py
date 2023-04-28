"""merge datasets"""
import os
from image_dataset import ImageDataset, make_dataset
import shutil

get_block_name = lambda index : "data_{}.bin".format(index)

def index_generator():
    index = 0
    while(True):
        yield index
        index += 1

def merge(src_dataset_paths:list[str], dst_dataset_path:str, chunksize):
    assert isinstance(src_dataset_paths, list), "src_dataset_path must be list"
    assert isinstance(chunksize, int), "chunksize must be int"
    assert all(os.path.exists(path) for path in src_dataset_paths), "at least one path in src_dataset_path doesn't exist"
    
    os.makedirs(dst_dataset_path, exist_ok=True)
    out_index_generator = index_generator()
    
    # move full datablocks
    for src_path in src_dataset_paths:
        dataset = ImageDataset(src_path, chunk_size=chunksize)
        num_full_blocks =  len(dataset) // chunksize
        for src_index in range(num_full_blocks):
            src_block_name = get_block_name(src_index)
            out_index = next(out_index_generator)
            dst_block_name = get_block_name(out_index)
            shutil.copy(os.path.join(src_path, src_block_name),
                        os.path.join(dst_dataset_path, dst_block_name))
            
    # merge datablocks that are not full
    def not_full_samples_generator():
        for src_path in src_dataset_paths:
            dataset = ImageDataset(src_path, chunk_size=chunksize)
            len_dataset = len(dataset)
            num_not_full_samples = len_dataset % chunksize
            for index in range(len_dataset - num_not_full_samples, len_dataset):
                yield dataset[index]
    temp_dataset_path = os.path.join(dst_dataset_path, "temp")
    os.makedirs(temp_dataset_path, exist_ok=True) # path to save temporary dataset
    with make_dataset(temp_dataset_path, chunk_size=chunksize) as ds:
        for img_meta in not_full_samples_generator():
            ds.write(img_meta.img, img_meta.meta)
    
    # move temporary datasets to dst_dataset_path
    for temp_index in range(len(os.listdir(temp_dataset_path))):
        temp_block_name = get_block_name(temp_index)
        out_index = next(out_index_generator)
        dst_block_name = get_block_name(out_index)
        shutil.copy(os.path.join(temp_dataset_path, temp_block_name),
                    os.path.join(dst_dataset_path, dst_block_name))
    shutil.rmtree(temp_dataset_path)

if __name__ == "__main__":
    from PIL import Image
    
    test_src_dataset_paths = ["../test_merge_dataset/dataset_src{}".format(k) for k in range(3)]
    test_dst_dataset_path = "../test_merge_dataset/merged"
    num_dataset = [12, 13, 14]  # numbers of samples for test_src_dataset
    chunk_size = 10

    for path in test_src_dataset_paths:
        os.makedirs(path, exist_ok=True)
    os.makedirs(test_dst_dataset_path, exist_ok=True)

    def img_meta_generator(num_img_meta):
        for k in range(num_img_meta):
            img = Image.new(mode="RGB", size=(100,100), color=0)
            meta = "meta{}".format(k)
            yield img, meta
    for index in range(3):
        with make_dataset(test_src_dataset_paths[index], chunk_size) as ds:
            for img, meta in img_meta_generator(num_dataset[index]):
                ds.write(img, meta)
    # merge
    merge(test_src_dataset_paths, test_dst_dataset_path, chunk_size)

    # test
    assert len(ImageDataset(test_dst_dataset_path, chunk_size)) == sum(num_dataset)   


        
    

    