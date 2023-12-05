/*_____________________ASIDE_BUTTONS____________________*/

document.querySelectorAll(".sidebar a[href]").forEach(link => {
  link.addEventListener('click', e => {
    e.preventDefault();
  });
})



const btns = document.querySelectorAll(".sidebar > a")
const windows = document.querySelectorAll("main>div:nth-child(n+3)")


var disp = new Map();
disp.set("insights", 'flex').set("rating-source", 'grid')


function change_aside(link) {
    btns.forEach(function(x) {x.classList.remove('active')})
    link.classList.add('active')
}

function change_main_window(link) {

  let window_name = link.getAttribute('window_id');
  w = document.getElementsByClassName(window_name)[0]
  if (w.style.display == 'none') {
    windows.forEach(function(x) {x.style.display = 'none'})
    windows.forEach(function(x) {x.style.opacity = 0})
    if (disp.has(window_name)) {
      w.style.display = disp.get(window_name)
    } else {
      w.style.display = 'block'
    }
    var op = 0
    while (op <= 1) {
      (function(op_){
        setTimeout(() => w.style.opacity = op_, 100 + 100 * op)
      })(op)
      op += 0.1
    }
  }  
}


btns.forEach(function(link) {
  console.log(link)
  link.addEventListener("click", function() {
    change_aside(link);
    change_main_window(link);
  })
});