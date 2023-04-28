"""split dataset"""
import os
import shutil
from merge import index_generator, get_block_name

def split(src_dataset_path:str, dst_dataset_paths:list[str], num_blocks:list[int]):
    assert isinstance(dst_dataset_paths, list) , "dst_dataset_paths should be list"
    assert isinstance(num_blocks, list) , "num_blocks should be list"
    assert all(isinstance(k, int) for k in num_blocks), "num_blocks should be integers"
    assert len(dst_dataset_paths) == len(num_blocks), "number of dst_dataset_paths should be the same as that of num_blocks"
    assert len(os.listdir(src_dataset_path)) == sum(num_blocks), "the sum of num_blocks should be equal to the number of blocks in src_dataset_path"

    input_index_generaor = index_generator()
    for dst_dataset_path, out_num_block in zip(dst_dataset_paths, num_blocks):
        for dst_index in range(out_num_block):
            dst_block_name = get_block_name(dst_index)
            src_index = next(input_index_generaor)
            src_block_name = get_block_name(src_index)
            shutil.copy(os.path.join(src_dataset_path, src_block_name),
                        os.path.join(dst_dataset_path,dst_block_name))

if __name__ == "__main__":
    from PIL import Image
    from image_dataset import ImageDataset,make_dataset
    test_src_dataset_path = "../test_split_dataset/dataset_src"
    test_dst_dataset_paths = ["../test_split_dataset/splited_{}".format(k) for k in range(3)]
    src_num_smple = 124
    num_split_blocks = [3, 4, 6]  # numbers of samples for test_src_dataset
    chunk_size = 10

    os.makedirs(test_src_dataset_path, exist_ok=True)
    for path in test_dst_dataset_paths:
        os.makedirs(path, exist_ok=True)

    def img_meta_generator(num_img_meta):
        for k in range(num_img_meta):
            img = Image.new(mode="RGB", size=(100,100), color=0)
            meta = "meta{}".format(k)
            yield img, meta
    with make_dataset(test_src_dataset_path, chunk_size) as ds:
        for img, meta in img_meta_generator(src_num_smple):
            ds.write(img, meta)
    # split
    split(test_src_dataset_path, test_dst_dataset_paths, num_split_blocks)
    