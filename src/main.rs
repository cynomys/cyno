use bio::io::fasta;

fn main() {
    let reader = fasta::Reader::from_file("/home/chad/ECI-2866.fasta").unwrap();
    for record in reader.records(){
        let r = record.unwrap();
        println!("{:?}", r);
        break;
    }
    println!("Done!");

}
