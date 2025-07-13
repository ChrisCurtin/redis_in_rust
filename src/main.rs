mod protocol;
mod command;


// TODO - this is scratch code as I try to figure out things in Rust
struct Point<X1:Clone, Y1:Clone> {
    x: X1,
    y: Y1,
}

impl<X1:Clone,Y1:Clone> Point<X1,Y1> {
    fn get_X(&self) -> &X1 {
        &self.x
    }
    fn get_Y(&self) -> &Y1 {
        &self.y
    }
    fn mixup<X2:Clone, Y2:Clone>(&self, other: &Point<X2, Y2>) -> Point<X1, Y2> {
        Point {
            x: self.get_X().clone(), // Assuming X1 implements Clone
            y: other.get_Y().clone(), // Assuming Y2 implements Clone
        }
    }
}


fn main() {
    let p1 = Point { x: 5, y: 10.4 };
    let p2 = Point { x: "Hello", y: 'c' };

    let p3 = p1.mixup(&p2);

    println!("p3.x = {}, p3.y = {}", p3.x, p3.y);
    println!("p1.x = {}, p1.y = {}", p1.x, p1.y);
    println!("p2.x = {}, p2.y = {}", p2.x, p2.y);
}
