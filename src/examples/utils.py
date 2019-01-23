import os
import shutil


def write_to_file(file_2_write, content):
    text_file = open(file_2_write, "w")
    text_file.write(content)
    text_file.close()


def write_df(df, path):

    def removeDir(path):
        if os.path.exists(path) and os.path.isdir(path):
            shutil.rmtree(path)
    removeDir(path)
    df.coalesce(1).write.format('json').save(path)


def delete_file(path):
    os.remove(path)


def drop_folder(folder):
    shutil.rmtree(folder)

