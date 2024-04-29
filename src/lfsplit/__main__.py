import argparse

from lfsplit import split_file, test_parameters


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-ifp', '--src-file-path', default = '../')                     #, default = 'data/input/'
    parser.add_argument('-ifn', '--src-file-name')                                      #, default = 'test_file.txt'
    parser.add_argument('-s', '--col-separator')                                        #, default = '|'
    #parser.add_argument('-h', '--header')                                          # Not implemented yet!
    parser.add_argument('-c', '--partition-col', default = 'col1')                      #, default = 'col57'
    parser.add_argument('-f', '--src-date-format', default = '%Y-%m-%d')                #, default = '%d.%m.%Y'
    parser.add_argument('-ofp', '--out-file-path', default = '../')                     #, default = 'data/output/'
    parser.add_argument('-ofn', '--out-file-name', default = 'lfsplit_output-*.txt')    #, default = 'test_file-*.txt'
    args = parser.parse_args()
    
    split_file(args)

    input('Press any key to exit')

# test if it's the main program
if __name__ == '__main__':
    main()
