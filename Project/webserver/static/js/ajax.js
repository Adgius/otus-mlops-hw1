import initPopup from './popup.js';

function update_table_content(json_text, start_table_length=0, update=true, idx=new Array()) {

	// json_text: комменты из таблицы для добавления в таблицу в формате {index1: text_comment1, index2: text_comment2, ...}
	// start_table_length: стартовый номер, с которого добавлять строки из json_text 
	// update: стереть предыдущие строки
	// idx: порядок индексов для json_text (при использовании similarity)

	let last_element = document.querySelector("#main-table-comments tbody tr:last-child")

	// Комменты приходят по 100 штук, если есть остаток, значит комменты кончились
	if (Object.keys(json_text).length < 100) {
		last_element.style.display = 'none';
	} else {
		last_element.style.display = 'table-cell';
	}
	if (update) {
		let new_tbody = document.createElement('tbody');
		let old_tbody = document.querySelector("#main-table-comments tbody")
		new_tbody.append(last_element)
		old_tbody.parentNode.replaceChild(new_tbody, old_tbody)
	}

	let add_row = function(key, value){
		value = value.replaceAll('\\"', '"');
  		let row = document.createElement('tr');
  		let content = document.createElement('td');
  		content.setAttribute("index", key)
  		let magic = document.createElement('td');
  		content.textContent = value;
  		magic.innerHTML = '<span class="material-icons-sharp">auto_fix_normal</span>'
  		row.appendChild(content)
  		row.appendChild(magic)
  		last_element.before(row)
	}
	if (idx.length > 0) {
		console.log(idx)
		for (let key of idx) {
			let value = json_text[key]
			add_row(key, value);
		}
	} else {
		if (Object.keys(json_text).length > 0) {
			for (let [key, value] of Object.entries(json_text).slice(start_table_length)) {
				console.log(key, value);
				add_row(key, value);
			}

		}		
	}
	initPopup();
	initSimilarity();
}	

// show more comments
document.getElementById("show_more_comments").onclick = function(e) {
	e.preventDefault();
	let xhr = new XMLHttpRequest();
	let table_length = document.querySelectorAll("#main-table-comments tbody tr").length - 1
	xhr.open('GET', `/show_more_comments?length=${table_length}&with_filter=${1}`)
	// xhr.setRequestHeader("Content-type", "application/x-www-form-urlencoded")
	xhr.onreadystatechange = function(){
		if(xhr.readyState == 4 && xhr.status==200){
			// console.log(JSON.parse(xhr.responseText));
			update_table_content(JSON.parse(xhr.responseText), table_length, false)
		}
	}
	xhr.send()
}


// query
document.getElementById("help-query-confirm").onclick = function(e) {
	e.preventDefault();
	let xhr = new XMLHttpRequest();
	let query = document.querySelector("#search-form textarea").value
	xhr.open('GET', `/execute_query?q=${query}`)
	xhr.onreadystatechange = function(){
		if(xhr.readyState == 4 && xhr.status==200){
			// console.log(xhr.responseText);
			update_table_content(JSON.parse(xhr.responseText), 0, true);
		}
	}
	xhr.send()
}


// show similarity
function initSimilarity() {
	var magic_buttons = document.querySelectorAll("#main-table-comments tbody .material-icons-sharp");
	magic_buttons.forEach(function(btn) {
		btn.onclick = function(e) {
			e.preventDefault();
			let xhr = new XMLHttpRequest();
			let index = e.target.parentNode.parentNode.querySelector("td:first-child").getAttribute("index")
			xhr.open('GET', `/get_sim_comments_from_table?index=${index}`)
			xhr.onreadystatechange = function(){
				if(xhr.readyState == 4 && xhr.status==200){
					// console.log(xhr.responseText);
					let pat = /(?<=\")\d+(?=\":\")/g
					let idx = xhr.responseText.match(pat);
					console.log(idx, isNaN(idx));
					update_table_content(JSON.parse(xhr.responseText), 0, true, idx);
				}
			}
			xhr.send()
		}
	})
}
initSimilarity();

