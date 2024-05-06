package main

import (
	"testing"
)

func Test_getFileDownloadUrlXj1(t *testing.T) {
	type args struct {
		ft FileTable
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "a",
			args: args{
				ft: FileTable{
					DW:   "xj",
					CFLJ: "http://11.71.10.44:8060/wbwj/scyx/jw/FT/xx.jpg",
				},
			},
			want:    "https://wbwj.http://11.71.10.44:8060/scyx/jw/FT/xx.jpg",
			wantErr: false,
		},
		{
			name: "b",
			args: args{
				ft: FileTable{
					DW:   "xj",
					CFLJ: "ftp://11.71.10.44:8060/wbwj/scyx/jw/FT/xx.jpg",
				},
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getFileDownloadUrlXj(tt.args.ft)
			if (err != nil) != tt.wantErr {
				t.Errorf("getFileDownloadUrlXj() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("getFileDownloadUrlXj() got = %v, want %v", got, tt.want)
			}
		})
	}
}
